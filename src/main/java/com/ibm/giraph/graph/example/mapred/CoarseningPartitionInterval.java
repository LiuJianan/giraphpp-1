package com.ibm.giraph.graph.example.mapred;

import java.io.*;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongCoarsenVertexValueLongMNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CoarseningPartitionInterval {
	public static class SMapper extends
			Mapper<LongWritable, LongCoarsenVertexValueLongMNeighborhood, LongWritable, LongWritable> {

		public void map(LongWritable key, LongCoarsenVertexValueLongMNeighborhood value, Context context)
				throws IOException, InterruptedException {
			
			long PartitionID = value.getVertexValue().value;
			
			long VertexID = key.get();
			
			
			
			context.write(new LongWritable(PartitionID), new LongWritable(VertexID));
			
		}
	}

	public static class SReducer extends
			Reducer<LongWritable, LongWritable, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			
			long PartitionID = key.get();
			
			long startVID = Long.MAX_VALUE;
			long endVID = Long.MIN_VALUE;
			
			for(LongWritable v : values)
			{
				startVID = Math.min(startVID, v.get());
				endVID = Math.max(endVID, v.get());
			}
			
			context.write(new LongWritable(PartitionID), new Text(startVID + " " + endVID  ));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CoarseningPartitionInterval");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Args: <in> <out>");
			System.exit(2);
		}
		
		job.setJarByClass(CoarseningPartitionInterval.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(SMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongCoarsenVertexValueLongMNeighborhood.class);
		
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setReducerClass(SReducer.class);
		job.setNumReduceTasks(90);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


