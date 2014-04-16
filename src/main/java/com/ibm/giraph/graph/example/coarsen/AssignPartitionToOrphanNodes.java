package com.ibm.giraph.graph.example.coarsen;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

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
import com.ibm.giraph.graph.example.ioformats.LongLongNullNeighborhood;
import com.ibm.giraph.graph.example.ioformats.LongCoarsenVertexValueLongMNeighborhood;
import com.ibm.giraph.graph.example.coarsen.PrepareMetisInputGraph.Counters;
import com.ibm.giraph.utils.MapRedudeUtils;

public class AssignPartitionToOrphanNodes implements Tool {

	public static final String NUM_PARTS="num.parts";
	private Configuration conf;
	
	protected static enum Counters {ORPHAN_NODES, NUM_OUTPUT_NODES, NUM_OUTPUT_EDGES };
	
	static class MyMapper extends Mapper<LongWritable, LongCoarsenVertexValueLongMNeighborhood, 
	LongWritable, LongLongNullNeighborhood>
	{
		LongLongNullNeighborhood outValue=new LongLongNullNeighborhood();
		Random rand=new Random(System.currentTimeMillis());
		private int numParts;
		protected void map(LongWritable key, LongCoarsenVertexValueLongMNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
			if(value.getVertexValue().state!=2 && (value.getVertexValue().value==1 && value.getNumberEdges()==0))
			{
				outValue.setVertexValue(-1-rand.nextInt(numParts));
				context.write(key, outValue);
			}
		}
		
		protected void setup(Context context) 
		{
			numParts=context.getConfiguration().getInt(NUM_PARTS, -1);
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			throw new IllegalArgumentException(
					"At least 2 arguments are requiered: <input> <output> <# partitions>");
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
		job.setJarByClass(AssignPartitionToOrphanNodes.class);		
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongLongNullNeighborhood.class);
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongLongNullNeighborhood.class);
		job.setNumReduceTasks(0);
		job.getConfiguration().setInt(NUM_PARTS, Integer.parseInt(args[2]));
		job.waitForCompletion(true);
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
		ToolRunner.run(new AssignPartitionToOrphanNodes(), args);
	}
}
