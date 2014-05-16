package com.ibm.giraph.graph.example.reachability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.graph.GiraphJob;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.DefaultMasterCompute;
import org.apache.giraph.graph.LongLongLongLongVertex;
import org.apache.giraph.graph.LongLongLongNeighborhood;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.Path;
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
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongLongVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.graph.example.reachability.ReachabilityVertex.SimpleLongLongLongVertexBinaryOutputFormat;
import com.ibm.giraph.utils.MapRedudeUtils;

public class ReachabilityGraph
	extends
	AbstractSubGraph<LongWritable, LongWritable, LongWritable, LongWritable>
	implements Tool {

    private static final Logger LOG = Logger.getLogger(ReachabilityGraph.class);
    private static String BFS_AGG = "BFS";
    public static int BFS_SOURCE = 0;
    public static int BFS_DEST = 0;
    private ArrayList<LongLongLongLongVertex> innerVertices = null;
    
    
    public void InitializeTagValue(
	    Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph)
	    throws IOException {

	innerVertices = new ArrayList<LongLongLongLongVertex>();
	Collection<BasicVertex<LongWritable, LongWritable, LongWritable, LongWritable>> vertices = subgraph
		.getVertices();

	Iterator<BasicVertex<LongWritable, LongWritable, LongWritable, LongWritable>> iterator = vertices
		.iterator();
	while (iterator.hasNext()) {
	    LongLongLongLongVertex vertex = (LongLongLongLongVertex) iterator
		    .next();
	    vertex.voteToHalt();
	    long tag = 0;
	    if (vertex.getVertexIdSimpleType() == ReachabilityGraph.BFS_SOURCE) {
		tag = 1;
		innerVertices.add(vertex);
	    } else if (vertex.getVertexIdSimpleType() == ReachabilityGraph.BFS_DEST) {
		tag = 2;
		innerVertices.add(vertex);
	    }
	    vertex.setVertexValueSimpleType(tag);
	}

    }

    private void BFS(
	    Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph) {
	// TODO Auto-generated method stub
	BooleanOrAggregator agg  = (BooleanOrAggregator) getAggregator(ReachabilityGraph.BFS_AGG);
	
	Queue<LongLongLongLongVertex> q1 = new LinkedList<LongLongLongLongVertex>();
	Queue<LongLongLongLongVertex> q2 = new LinkedList<LongLongLongLongVertex>();

	Iterator<LongLongLongLongVertex> iterator = innerVertices.iterator();

	while (iterator.hasNext()) {
	    LongLongLongLongVertex v = iterator.next();
	    if (v.getVertexValueSimpleType() == 1)
		q1.add(v);
	    else if (v.getVertexValueSimpleType() == 2)
		q2.add(v);
	}

	while (!q1.isEmpty()) {
	    LongLongLongLongVertex cur = q1.poll();

	    for (int i = 0; i < cur.getNumOutEdges(); i++) {
		LongWritable vid = cur.getEdgeID(i);
		long value = cur.getSimpleEdgeValue(i);

		if (value == 1)
		    continue; // only out edges

		LongLongLongLongVertex nvertex = (LongLongLongLongVertex) subgraph
			.getVertex(vid);

		if (nvertex == null) {
		    cur.sendMsg(vid, cur.getVertexValue());
		} else {
		    long tag = nvertex.getVertexValueSimpleType();
		    if ((tag | 1) != tag) {
			tag |= 1;
			nvertex.setVertexValueSimpleType(tag);
			if (tag == 3) {
			    agg.aggregate(true);
			} else
			    q1.add(nvertex);
		    }
		}
	    }
	}

	while (!q2.isEmpty()) {
	    LongLongLongLongVertex cur = q2.poll();

	    for (int i = 0; i < cur.getNumOutEdges(); i++) {
		LongWritable vid = cur.getEdgeID(i);
		long value = cur.getSimpleEdgeValue(i);

		if (value == 0)
		    continue; // only in edges

		LongLongLongLongVertex nvertex = (LongLongLongLongVertex) subgraph
			.getVertex(vid);

		if (nvertex == null) {
		    cur.sendMsg(vid, cur.getVertexValue());
		} else {
		    long tag = nvertex.getVertexValueSimpleType();
		    if ((tag | 2) != tag) {
			tag |= 2;
			nvertex.setVertexValueSimpleType(tag);
			if (tag == 3) {
			    agg.aggregate(true);
			} else
			    q2.add(nvertex);
		    }
		}
	    }
	}

    }

    private void updateLabel(
	    Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph) {
	Iterator<BasicVertex<LongWritable, LongWritable, LongWritable, LongWritable>> iterator = subgraph
		.getVertices().iterator();

	innerVertices = new ArrayList<LongLongLongLongVertex>();
	// process all the messages to update label mappings
	while (iterator.hasNext()) {
	    LongLongLongLongVertex vertex = (LongLongLongLongVertex) iterator
		    .next();
	    vertex.voteToHalt();
	    long[] list = vertex.getMessagesSimpleType();
	    if (list.length == 0)
		continue;
	    // get min
	    long oldtag = vertex.getVertexValueSimpleType();
	    long newtag = oldtag;
	    for (int i = 0; i < list.length; i++)
		newtag |= list[i];

	    if (newtag == oldtag)
		continue;
	    vertex.setVertexValueSimpleType(newtag);
	    innerVertices.add(vertex);
	}
    }

    @Override
    public void compute(
	    final Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph)
	    throws IOException {

	innerVertices = null;
	BooleanOrAggregator agg  = (BooleanOrAggregator) getAggregator(ReachabilityGraph.BFS_AGG);
	
	if(agg.getAggregatedValue().get())
	{
		return;
	}

	if (this.getSuperstep() == 0) {
	    InitializeTagValue(subgraph);
	    BFS(subgraph);
	} else {
	    updateLabel(subgraph);
	    BFS(subgraph);
	}

    }
	public static class ReachabilityWorkerContext extends WorkerContext {

		private static boolean FINAL_BFS;

		public static boolean getFinalStar() {
			return FINAL_BFS;
		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
		}

		@Override
		public void postApplication() {

			BooleanOrAggregator agg  = (BooleanOrAggregator) getAggregator(ReachabilityGraph.BFS_AGG);
			FINAL_BFS = agg.getAggregatedValue().get();
		}

		@Override
		public void preSuperstep() {

		}

		@Override
		public void postSuperstep() {
		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class ReachabilityMasterCompute extends
			DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(BFS_AGG, BooleanOrAggregator.class);
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
	GiraphJob job = new GiraphJob(getConf(), "ReachabilityGraph");
	job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
	job.setVertexClass(LongLongLongLongVertex.class);
	job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
		getClass(), SubGraph.class);

	job.setVertexInputFormatClass(SimpleLongLongLongVertexBinaryInputFormat.class);
	job.setVertexOutputFormatClass(SimpleLongLongLongVertexBinaryOutputFormat.class);
	FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
	Path outpath = new Path(args[1]);
	FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
	MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());

	KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(),
		SimpleLongLongLongVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
	KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(),
		LongLongLongNeighborhood.class);

	job.setVertexCombinerClass(BitOrCombiner.class);

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
	System.exit(ToolRunner.run(new ReachabilityGraph(), args));
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
