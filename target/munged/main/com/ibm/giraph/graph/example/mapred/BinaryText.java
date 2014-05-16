package com.ibm.giraph.graph.example.mapred;

import java.io.*;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BinaryText {
	public static class SMapper extends
			Mapper<LongWritable, SkeletonNeighborhood, LongWritable, Text> {

		public void map(LongWritable key, SkeletonNeighborhood value, Context context)
				throws IOException, InterruptedException {
			
			long VertexID = key.get();
			int num = value.getNumberEdges();
			
			StringBuilder sb = new StringBuilder();
			sb.append(num);
			for(int i = 0 ; i < num ; i ++)
			{
				sb.append(" ");
				sb.append(value.getEdgeID(i));
			}
			 
			context.write(new LongWritable(VertexID), new Text(sb.toString()));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "BinaryText");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Args: <in> <out>");
			System.exit(2);
		}
		
		job.setJarByClass(BinaryText.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setMapperClass(SMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SkeletonNeighborhood.class);
		
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


