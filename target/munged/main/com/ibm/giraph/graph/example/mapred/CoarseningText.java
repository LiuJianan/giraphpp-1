package com.ibm.giraph.graph.example.mapred;

import java.io.*;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.LongCoarsenVertexValueLongMNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CoarseningText {
	public static class SMapper extends
			Mapper<LongWritable, LongCoarsenVertexValueLongMNeighborhood, LongWritable, Text> {

		public void map(LongWritable key, LongCoarsenVertexValueLongMNeighborhood value, Context context)
				throws IOException, InterruptedException {
			
			long VertexID = key.get();
			int num = value.getNumberEdges();
			
			byte state = value.getVertexValue().state;
			long val = value.getVertexValue().value;
			
			StringBuilder sb = new StringBuilder();
			sb.append(state + " " + val + " " + num);
			for(int i = 0 ; i < num ; i ++)
			{
				sb.append(" " + value.getEdgeID(i) + " "  + value.getEdgeValue(i));
			}
			 
			context.write(new LongWritable(VertexID), new Text(sb.toString()));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "CoarseningText");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Args: <in> <out>");
			System.exit(2);
		}
		
		job.setJarByClass(CoarseningText.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setMapperClass(SMapper.class);
		job.setMapOutputKeyClass(TextInputFormat.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongCoarsenVertexValueLongMNeighborhood.class);
		
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


