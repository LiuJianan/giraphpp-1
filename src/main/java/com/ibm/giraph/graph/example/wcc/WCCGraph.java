package com.ibm.giraph.graph.example.wcc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongLongNullLongVertex;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongLongNullNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongNullLongBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongNullXBinaryVertexInputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.graph.example.wcc.WCCVertex.LongLongMinCombiner;
import com.ibm.giraph.graph.example.wcc.WCCVertex.SimpleLongLongNullVertexBinaryOutputFormat;
import com.ibm.giraph.utils.MapRedudeUtils;

public class WCCGraph extends AbstractSubGraph<LongWritable, LongWritable, NullWritable, LongWritable> 
implements Tool{

	private ChangedLabels changedLabels=new ChangedLabels();
	private LongWritable longbuff=new LongWritable();
	SequentialWCC singleWcc=new SequentialWCC();
		
	private void updateLabelMapping(Partition<LongWritable, LongWritable, NullWritable, LongWritable> subgraph)
	{
		Iterator<BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable>> 
		iterator=subgraph.getVertices().iterator();
		
		//process all the messages to update label mappings
		while (iterator.hasNext()) {
			LongLongNullLongVertex vertex = (LongLongNullLongVertex) iterator.next();
			vertex.voteToHalt();
			
			long[] list=vertex.getMessagesSimpleType();
			if(list.length==0)
				continue;
			
			//get min
			long oldLabel=vertex.getVertexValueSimpleType();
			long min=oldLabel;
			for(int i=0; i<list.length; i++)
				if(min>list[i])
					min=list[i];
			if(min>=oldLabel)//if vertex label is not changed, just go on to the next vertex
				continue;
			
			//update vertex label, only if found smaller mapped label
			if(!changedLabels.labels.containsKey(oldLabel) || changedLabels.labels.get(oldLabel)>min)
				changedLabels.labels.put(oldLabel, min);
			
		}
		changedLabels.consolidate();
	}
	
	private void updateVertexLabels(Partition<LongWritable, LongWritable, NullWritable, LongWritable> subgraph)
	{
		Iterator<BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable>> 
		iterator=subgraph.getVertices().iterator();
		while (iterator.hasNext()) {
			final LongLongNullLongVertex vertex = (LongLongNullLongVertex) iterator.next();
			
			long oldLabel=vertex.getVertexValueSimpleType();
			if(changedLabels.labels.containsKey(oldLabel))
			{
				final long newLabel=changedLabels.labels.get(oldLabel);
				if(newLabel>=oldLabel) continue;
				
				vertex.setVertexValueSimpleType(newLabel);
				
				//update labels of all external neighbors
				long[] edges=vertex.getNeighborsSimpleType();
				LongWritable msg=new LongWritable(newLabel);
				for(long dest: edges)
				{
					longbuff.set(dest);
					if(subgraph.getVertex(longbuff)==null)
					{
						vertex.sendMsg(new LongWritable(dest), msg);	
					}
				}
			}
		}
	}
	
	@Override
	public void compute(
			Partition<LongWritable, LongWritable, NullWritable, LongWritable> subgraph)
			throws IOException {
		if(getSuperstep()==0)
		{
			//run a single node wcc algrithm
			singleWcc.computeWCC(subgraph);
			singleWcc.reset();
		}else
		{
			updateLabelMapping(subgraph);
			updateVertexLabels(subgraph);
		}
			
	}

	private static class MyVertex extends LongLongNullLongVertex
	{
		@Override
		public void compute(Iterator<LongWritable> msgIterator)
				throws IOException {
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
		GiraphJob job = new GiraphJob(getConf(), "WCCGraph");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(MyVertex.class);
		job.setVertexInputFormatClass(SimpleLongLongNullLongBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongLongNullVertexBinaryOutputFormat.class);
		job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
				getClass(), SubGraph.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongLongNullXBinaryVertexInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongLongNullNeighborhood.class);
			job.setVertexCombinerClass(LongLongMinCombiner.class);
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

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new WCCGraph(), args);
	}
	
	@Override
	public void readAuxiliaryDataStructure(DataInput input) throws IOException {
		changedLabels.readFields(input);
	}

	@Override
	public void writeAuxiliaryDataStructure(DataOutput output)
			throws IOException {
		changedLabels.write(output);
	}
}
