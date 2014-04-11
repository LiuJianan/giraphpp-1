package com.ibm.giraph.graph.example.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongDoubleNullDoubleVertex;
import org.apache.giraph.graph.LongDoubleNullNeighborhood;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongDoubleNullDoubleBinaryInputFormat;
import com.ibm.giraph.graph.example.pagerank.DeltaPRVertex.SimpleLongDoubleNullVertexBinaryOutputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.utils.MapRedudeUtils;

public class DeltaPRGraph extends AbstractSubGraph<LongWritable, DoubleWritable, NullWritable, DoubleWritable> 
implements Tool{

	private LongWritable longbuff=new LongWritable();
	public static final int MAX_SUPERSTEPS=10;
	private static final Logger LOG =Logger.getLogger(DeltaPRGraph.class);
	@Override
	public void compute(
			final Partition<LongWritable, DoubleWritable, NullWritable, DoubleWritable> subgraph)
			throws IOException {
		long superstep=getSuperstep();
		
		Iterator<BasicVertex<LongWritable, DoubleWritable, NullWritable, DoubleWritable>> 
		iterator=subgraph.getVertices().iterator();
		
		while (iterator.hasNext()) {
			
			MyVertex vertex = (MyVertex) iterator.next();
			if(superstep>= MAX_SUPERSTEPS)//stop
			{
				vertex.voteToHalt();
				continue;
			}
			
			if(superstep==0)
			{
				vertex.setVertexValueSimpleType(0);
				vertex.delta+=0.15;
			}
			
			double[] list=vertex.getMessagesSimpleType();
			//delta from messages
			for(int i=0; i<list.length; i++)
				vertex.delta+=list[i];
			
			if(vertex.delta==0)
				continue;
			
			vertex.setVertexValueSimpleType(vertex.getVertexValueSimpleType()+vertex.delta);
			final double updateToNeighbor=0.85*vertex.delta/(double)vertex.getNumOutEdges();
			//clear the entry in the cache
			vertex.delta=0;
			for(long dest: vertex.getNeighborsSimpleType())
			{
				longbuff.set(dest);
				MyVertex neighbor=(MyVertex) subgraph.getVertex(longbuff);
				if(neighbor==null)
					sendMsg(new LongWritable(dest), new DoubleWritable(updateToNeighbor));
				else
					neighbor.delta+=updateToNeighbor;
			}
		}
	}
	
	public static class MyVertex extends LongDoubleNullDoubleVertex
	{
		public double delta=0;

		@Override
		public void compute(Iterator<DoubleWritable> msgIterator)
				throws IOException {
		}
		
		@Override
	    public void write(final DataOutput out) throws IOException {
			super.write(out);
			out.writeDouble(delta);
		}
		
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			delta=in.readDouble();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println(
					"run: Must have 5 arguments <input path> <output path> <# of workers> <hash partition: true, range partition: false> "
					+ "<if hash partition, #partitions, otherwise range partiton file>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "DeltaPRGraph");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(MyVertex.class);
		job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
				getClass(), SubGraph.class);
		
		job.setVertexInputFormatClass(SimpleLongDoubleNullDoubleBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongDoubleNullVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongDoubleNullDoubleBinaryInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongDoubleNullNeighborhood.class);
		
		job.setVertexCombinerClass(SumCombiner.class);
		
		if(Boolean.parseBoolean(args[3]))
			job.getConfiguration().setInt(HashMasterPartitioner.USER_PARTITION_COUNT, Integer.parseInt(args[4]));
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
		System.exit(ToolRunner.run(new DeltaPRGraph(), args));
	}

	@Override
	public void readAuxiliaryDataStructure(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeAuxiliaryDataStructure(DataOutput output)
			throws IOException {
		// TODO Auto-generated method stub
		
	}
}
