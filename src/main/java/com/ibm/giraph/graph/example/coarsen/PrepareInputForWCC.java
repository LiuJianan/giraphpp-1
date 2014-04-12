package com.ibm.giraph.subgraph.example.coarsen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import com.ibm.giraph.formats.binary.KVBinaryInputFormat;
import com.ibm.giraph.formats.binary.KVBinaryOutputFormat;
import com.ibm.giraph.formats.binary.LongParMetisVertexValueLongMNeighborhood;
import com.ibm.giraph.formats.binary.SkeletonNeighborhood;
import com.ibm.giraph.subgraph.io.StanfordReader;
import com.ibm.giraph.subgraph.io.StanfordReader.InnerReducer;

public class PrepareInputForWCC implements Tool {

	private Configuration conf;
	
	static class MyMapper extends Mapper<LongWritable, LongParMetisVertexValueLongMNeighborhood, 
	LongWritable, LongWritable>
	{
		LongWritable buff=new LongWritable();
		protected void map(LongWritable key, LongParMetisVertexValueLongMNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
		//	Log.info("mapper input: "+key+" "+value.getVertexValue());
			if(value.getVertexValue().state==2)
			{
				buff.set(value.getVertexValue().value);
				context.write(key, buff);
				context.write(buff, key);
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("At least 4 arguments are requiered: <coarsen result graph> <metis parititon mapping> <OutputPath> <#Reducers>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJobName(this.getClass().getName());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongParMetisVertexValueLongMNeighborhood.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		Path outpath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outpath);
		job.setReducerClass(InnerReducer.class);
		job.setJarByClass(PrepareInputForWCC.class);
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		job.getConfiguration().setInt(StanfordReader.REQUIRED_OUTPUT_FORMAT, 2);
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(SkeletonNeighborhood.class);
		
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}	
		job.waitForCompletion(true);
		return 0;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration cf) {
		conf=cf;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new PrepareInputForWCC(), args);
	}
}
