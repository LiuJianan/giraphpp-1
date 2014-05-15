package com.ibm.giraph.graph.example.mapred;

import java.io.*;
import java.util.Vector;

import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;

import org.apache.giraph.graph.LongDoubleDoubleNeighborhood;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class TextBinaryEdgeValueMerge {
	public static class SMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\\s+");

			context.write(new LongWritable(Long.parseLong(tokens[0])), value);

		}
	}

	public static class SReducer
			extends
			Reducer<LongWritable, Text, LongWritable, LongDoubleDoubleNeighborhood> {

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			LongDoubleDoubleNeighborhood val = new LongDoubleDoubleNeighborhood();

			String[] nbs = values.iterator().next().toString().split("\\s+");
			int numofEdges = Integer.parseInt(nbs[1]);
			long[] edges = new long[numofEdges];
			double[] edgeValues = new double[numofEdges];
			for (int i = 0; i < numofEdges; i++) {
				edges[i] = Long.parseLong(nbs[2 * i + 2]);
				edgeValues[i] = Double.parseDouble(nbs[2 * i + 3]);
			}

			val.setSimpleEdges(edges, edgeValues);

			context.write(new LongWritable(Long.parseLong(nbs[0])), val);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TextBinaryMerge");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Args: <in> <out> <num>");
			System.exit(3);
		}

		job.setJarByClass(TextBinaryEdgeValueMerge.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);// cnt
		job.setMapOutputValueClass(Text.class);// vid

		job.setReducerClass(SReducer.class);

		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongDoubleDoubleNeighborhood.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}