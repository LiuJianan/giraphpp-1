package com.ibm.giraph.subgraph.example.coarsen;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import com.ibm.giraph.formats.binary.KVBinaryInputFormat;
import com.ibm.giraph.formats.binary.KVBinaryOutputFormat;
import com.ibm.giraph.formats.binary.LongLongNullNeighborhood;
import com.ibm.giraph.subgraph.example.LongTotalOrderPartiitoner;

public class FinalPartitionMapping implements Tool {

	private static final String PART_COUNTER_GROUP="#Nodes in Group";
//	public static final String PART_FILE_NAME="part.file.name";
	private Configuration conf;
	
	static class MyMapper extends Mapper<LongWritable, LongLongNullNeighborhood, 
	LongWritable, LongWritable>
	{
		LongWritable buff=new LongWritable();
		protected void map(LongWritable key, LongLongNullNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
			//Log.info("@@ mapper input: "+key+" "+value);
			if(key.get()>=0)//real vertex
			{
				buff.set(-1-value.getVertexValue());
				context.write(buff, key);
			//	Log.info("mapper out: "+buff+" "+key);
			}
		}
	}
	
	//need 1 reducer
	static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, NullWritable>
	{
	//	int n=0;
	//	int i=0;
	//	String outStr="";
		public void reduce(LongWritable partition, Iterable<LongWritable> values, Context context) 
		throws IOException, InterruptedException 
		{
			//outStr+=i+"\t"+n+"\n";
			int num=0;
			for(LongWritable value: values)
			{
				context.write(value, NullWritable.get());
				//Log.info("reducer out: "+value);
				//n++;
				num++;
			}
			//i++;
			context.getCounter(PART_COUNTER_GROUP, partition.toString()).increment(num);
		}
		public void cleanup(Context context)
		throws IOException, InterruptedException 
		{
			/*String file=context.getConfiguration().get(PART_FILE_NAME);
			FileSystem fs=FileSystem.get(context.getConfiguration());
			Path partPath=new Path(file);
			fs.delete(partPath, false);
			BufferedWriter out=new BufferedWriter( new OutputStreamWriter(fs.create(partPath)));
			out.write(outStr);
			out.close();*/
			super.cleanup(context);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("At least 3 arguments are requiered: <wcc result> <orphan nodes> <OutputPath> <parition boundaries> <num partitions> <num Reducers>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJobName(this.getClass().getName());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongLongNullNeighborhood.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		Path outpath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outpath);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(FinalPartitionMapping.class);
		job.setNumReduceTasks(Integer.parseInt(args[5]));
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), NullWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}
		//job.getConfiguration().set(PART_FILE_NAME, args[3]);
		job.setPartitionerClass(LongTotalOrderPartiitoner.class);
		LongTotalOrderPartiitoner.setNumKeys(job.getConfiguration(), Long.parseLong(args[4]));
		job.waitForCompletion(true);
		
		Counters counters = job.getCounters();
		CounterGroup group = counters.getGroup(PART_COUNTER_GROUP);
		Iterator<Counter> iterator = group.iterator();
		TreeMap<Integer, Long> counterMap = new TreeMap<Integer, Long>();
		while (iterator.hasNext()) {
			Counter c = iterator.next();
			counterMap.put(Integer.parseInt(c.getName()), c.getValue());
		}
		FileSystem fs=FileSystem.get(job.getConfiguration());
		Path partPath=new Path(args[3]);
		fs.delete(partPath, false);
		BufferedWriter out=new BufferedWriter( new OutputStreamWriter(fs.create(partPath)));
		int i=0;
		long n=0;
		for(Long num: counterMap.values())
		{
			out.write(i+"\t"+n+"\n");
			i++;
			n+=num;
		}
		out.close();
		
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
		ToolRunner.run(new FinalPartitionMapping(), args);
	}
}
