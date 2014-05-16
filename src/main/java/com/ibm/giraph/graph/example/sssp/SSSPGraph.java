package com.ibm.giraph.graph.example.sssp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongDoubleDoubleDoubleVertex;
import org.apache.giraph.graph.LongDoubleDoubleNeighborhood;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongDoubleDoubleVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.graph.example.sssp.SSSPVertex.SimpleLongDoubleDoubleVertexBinaryOutputFormat;
import com.ibm.giraph.utils.MapRedudeUtils;

public class SSSPGraph
	extends
	AbstractSubGraph<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>
	implements Tool {

    private static final Logger LOG = Logger.getLogger(SSSPGraph.class);
    private ArrayList<LongDoubleDoubleDoubleVertex> innerVertices = null;

    private boolean isSource(long id) {

	return id == 0;
    }

    private void InitializeDistance(
	    Partition<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> subgraph) {

	innerVertices = new ArrayList<LongDoubleDoubleDoubleVertex>();

	Collection<BasicVertex<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>> vertices = subgraph
		.getVertices();
	Iterator<BasicVertex<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>> iterator = vertices
		.iterator();

	while (iterator.hasNext()) {
	    LongDoubleDoubleDoubleVertex vertex = (LongDoubleDoubleDoubleVertex) iterator
		    .next();
	    vertex.voteToHalt();

	    if (isSource(vertex.getVertexIdSimpleType())) {
		vertex.setVertexValueSimpleType(0);
		innerVertices.add(vertex);
	    } else {
		vertex.setVertexValueSimpleType(Double.MAX_VALUE);
	    }
	}

    }

    private void updateLabel(
	    Partition<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> subgraph) {
	// TODO Auto-generated method stub
	Iterator<BasicVertex<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>> iterator = subgraph
		.getVertices().iterator();

	innerVertices = new ArrayList<LongDoubleDoubleDoubleVertex>();
	// process all the messages to update label mappings
	while (iterator.hasNext()) {
	    LongDoubleDoubleDoubleVertex vertex = (LongDoubleDoubleDoubleVertex) iterator
		    .next();
	    vertex.voteToHalt();
	    double[] list = vertex.getMessagesSimpleType();
	    if (list.length == 0)
		continue;
	    // get min
	    double olddis = vertex.getVertexValueSimpleType();
	    double newdis = olddis;
	    for (int i = 0; i < list.length; i++)
		newdis = Math.min(newdis, list[i]);

	    if (newdis >= olddis)
		continue;
	    vertex.setVertexValueSimpleType(newdis);
	    innerVertices.add(vertex);
	}
    }

    private void Dijkstra(
	    Partition<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> subgraph) {
	// TODO Auto-generated method stub
	
	HashMap<Long,Integer> vidmap = new HashMap<Long,Integer>();
	
	Iterator<BasicVertex<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>> it = subgraph.getVertices().iterator();
	
	while(it.hasNext())
	{
	    LongDoubleDoubleDoubleVertex v = (LongDoubleDoubleDoubleVertex) it.next();
	    int size = vidmap.size();
	    vidmap.put(v.getVertexIdSimpleType(), size + 1);
	}
	
	Iterator<LongDoubleDoubleDoubleVertex> iterator = innerVertices.iterator();

	QEle[] ele = new QEle[subgraph.getVertices().size()];
	MinHeap hp = new MinHeap();
	while(iterator.hasNext())
	{
	    LongDoubleDoubleDoubleVertex v = (LongDoubleDoubleDoubleVertex) iterator.next();
	    int idx = vidmap.get(v.getVertexIdSimpleType());
	    ele[idx] = new QEle(idx, v);
	    hp.add(ele[idx]);
	}
	
        //run dijkstra's alg
        while (hp.size() > 0)
        {
            LongDoubleDoubleDoubleVertex cur = hp.remove().key;
            if (cur.getVertexValueSimpleType() == Double.MAX_VALUE )
                break;

            for (int i = 0; i < cur.getNumOutEdges(); i++) {
		LongWritable vid = cur.getEdgeID(i);
		double edgeWeight = cur.getSimpleEdgeValue(i);
		double dis = cur.getVertexValueSimpleType() + edgeWeight;

		LongDoubleDoubleDoubleVertex nvertex = (LongDoubleDoubleDoubleVertex) subgraph
			.getVertex(vid);

		if (nvertex == null) {
		    cur.sendMsg(vid, new DoubleWritable(dis));
		} else {
		    
		    if(dis < nvertex.getVertexValueSimpleType())
		    {
			int idx = vidmap.get(nvertex);
			QEle e = ele[idx];
			if(e == null)
			{
			    ele[idx] = new QEle(idx, nvertex);
			    hp.add(ele[idx]);
			}
			else
			{
			    nvertex.setVertexValueSimpleType(dis);
			    hp.fix(e);
			}
		    }
		}
	    }
           
        }
    }

    @Override
    public void compute(
	    final Partition<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> subgraph)
	    throws IOException {
	innerVertices = null;

	if (this.getSuperstep() == 0) {
	    InitializeDistance(subgraph);
	    Dijkstra(subgraph);
	} else {
	    updateLabel(subgraph);
	    Dijkstra(subgraph);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	if (args.length != 5) {
	    System.err
		    .println("run: Must have 5 arguments <input path> <output path> <# of workers> <hash partition: true, range partition: false> "
			    + "<if hash partition, #partitions, otherwise range partiton file>");
	    System.exit(-1);
	}
	GiraphJob job = new GiraphJob(getConf(), "SSSPGraph");
	job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
	job.setVertexClass(LongDoubleDoubleDoubleVertex.class);
	job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
		getClass(), SubGraph.class);

	job.setVertexInputFormatClass(SimpleLongDoubleDoubleVertexBinaryInputFormat.class);
	job.setVertexOutputFormatClass(SimpleLongDoubleDoubleVertexBinaryOutputFormat.class);
	FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
	Path outpath = new Path(args[1]);
	FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
	MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());

	KVBinaryInputFormat
		.setInputNeighborhoodClass(
			job.getConfiguration(),
			SimpleLongDoubleDoubleVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
	KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(),
		LongDoubleDoubleNeighborhood.class);

	job.setVertexCombinerClass(MinCombiner.class);

	if (Boolean.parseBoolean(args[3]))
	    job.getConfiguration().setInt(
		    HashMasterPartitioner.USER_PARTITION_COUNT,
		    Integer.parseInt(args[4]));
	else
	    MyLongRangePartitionerFactory.setRangePartitioner(job, args[4]);
	job.setWorkerConfiguration(Integer.parseInt(args[2]),
		Integer.parseInt(args[2]), 100.0f);
	if (job.run(true) == true) {
	    return 0;
	} else {
	    return -1;
	}
    }

    public static void main(String[] args) throws Exception {
	System.exit(ToolRunner.run(new SSSPGraph(), args));
    }

    @Override
    public void readAuxiliaryDataStructure(DataInput arg0) throws IOException {
	// TODO Auto-generated method stub

    }

    @Override
    public void writeAuxiliaryDataStructure(DataOutput arg0) throws IOException {
	// TODO Auto-generated method stub

    }

}
