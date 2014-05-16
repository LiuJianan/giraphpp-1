package com.ibm.giraph.graph.example.coarsen;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.map.OpenLongIntHashMap;
import org.mortbay.log.Log;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongCoarsenVertexValueLongMNeighborhood;
import com.ibm.giraph.graph.example.coarsen.PrepareMetisInputGraph.Counters;
import com.ibm.giraph.utils.MapRedudeUtils;

public class PrepareMetisInputMapping implements Tool {

	private Configuration conf;
	
	protected static enum Counters {ORPHAN_NODES, NUM_OUTPUT_NODES, NUM_OUTPUT_EDGES };
	
	static class MyMapper extends Mapper<LongWritable, LongCoarsenVertexValueLongMNeighborhood, LongWritable, NullWritable>
	{
		long orphanNodes=0;
		long numnodes=0;
		long numEdges=0;
		protected void map(LongWritable key, LongCoarsenVertexValueLongMNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
			if(value.getVertexValue().state==2 || (value.getVertexValue().value==1 && value.getNumberEdges()==0))
			{
			//	Log.info("mapper ignore: "+key);
				return;
			}
			else
			{
				context.write(key, NullWritable.get());
				numnodes++;
				numEdges+=value.getNumberEdges();
			//	if(value.getNumberEdges()==0)
			//		orphanNodes++;
			//	if(value.getVertexValue().state!=1)
			//		Log.info("!!!!!!!!! odd node: "+key+": "+value);
			}
		//	Log.info("mapper: "+value);
		}
		
		public void cleanup(Mapper.Context context)
		throws IOException, InterruptedException
		{
			context.getCounter(Counters.NUM_OUTPUT_NODES).increment(numnodes);
			context.getCounter(Counters.NUM_OUTPUT_EDGES).increment(numEdges);
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, NullWritable, NullWritable, Text>
	{	
		Text textbuff=new Text();
		int numNodes=0; 
		//int numEdges=0;
		
		public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException 
		{
			numNodes++;
			textbuff.set(key.get()+" "+numNodes);
			context.write(NullWritable.get(), textbuff);
		//	Log.info("reducer: "+nbhd);
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			throw new IllegalArgumentException(
					"At least 2 arguments are requiered: <input> <mapping output> <local metis file>");
		}
		Job job = new Job();
		job.setJobName(this.getClass().getName());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outpath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongCoarsenVertexValueLongMNeighborhood.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(PrepareMetisInputMapping.class);		
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		long numNodes=job.getCounters().findCounter(Counters.NUM_OUTPUT_NODES).getValue();
		long numEdges=job.getCounters().findCounter(Counters.NUM_OUTPUT_EDGES).getValue();
		File f=new File(args[2]);
		if(f.exists())
			f.delete();
		BufferedWriter out=new BufferedWriter(new FileWriter(f));
		out.write(numNodes+" "+(numEdges/2)+" 011\n");
		out.close();
		return 0;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf=conf;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new PrepareMetisInputMapping(), args);
	}
}
