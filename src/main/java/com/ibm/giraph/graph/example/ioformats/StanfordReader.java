package com.ibm.giraph.graph.example.ioformats;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Partitioner;

import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongDoubleFloatNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;

public class StanfordReader implements Tool
{
	public static final String CHANGE_TO_UNDIRECTED_GRAPH="change.to.undirected.graph";
	public static final String REQUIRED_OUTPUT_FORMAT="required.output.format";
	public static final String RANGE_PARTITION_BOUNDARIES="range.partition.boundaries";
	Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration cf) {
		this.conf = cf;
	}

	static class InnerMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
	{
		private LongWritable from=new LongWritable();
		private LongWritable to=new LongWritable();
		private boolean changeToUndirectedGraph=false;
		protected void map(LongWritable key, Text value,  Context context)
		{
			String line=value.toString();
			if (line.startsWith("#")) return;//comments
			String[] splits = line.split("\t");
			if(splits.length<2)
				throw new RuntimeException(line+" is ill formated!");
			
			from.set(Long.parseLong(splits[0]));
			to.set(Long.parseLong(splits[1]));
			try {
				context.write(from, to);
				if(changeToUndirectedGraph)
					context.write(to, from);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
		}
		protected void setup(Context context) 
		{
			changeToUndirectedGraph=context.getConfiguration().getBoolean(CHANGE_TO_UNDIRECTED_GRAPH, false);
		}
	}
	
	public static class InnerReducer extends Reducer<LongWritable, LongWritable, WritableComparable, Writable>
	{
		private int outputformat=0;//0: text, 1:binary vertex output
		WritableComparable outkey=null;
		Writable outValue=null;
		ArrayList<Long> edges=new ArrayList<Long>();
		protected void reduce(LongWritable key, Iterable<LongWritable> values,  Context context)
		{
			Iterator<LongWritable> valueiterator = values.iterator();
			long oldValue=Long.MIN_VALUE;
			if(outputformat==0)
			{
				String str="";
				while (valueiterator.hasNext())
				{
					LongWritable value=valueiterator.next();
					if(value.get()==oldValue)
						continue;
					str+=key+"\t"+value+"\n";
					oldValue=value.get();
				}
				((Text)outValue).set(str.substring(0, str.length()-1));
			}else if(outputformat==1)
			{
				edges.clear();
				while (valueiterator.hasNext())
				{
					LongWritable value=valueiterator.next();
					if(value.get()==oldValue)
						continue;
					edges.add(value.get());
					oldValue=value.get();
				}
				((LongDoubleFloatNeighborhood) outValue).setSimpleEdges(edges);
				((LongDoubleFloatNeighborhood) outValue).setVertexValue(0);
				((LongWritable) outkey).set(key.get());
			}else if(outputformat==2)
			{
				edges.clear();
				while (valueiterator.hasNext())
				{
					LongWritable value=valueiterator.next();
					if(value.get()==oldValue)
						continue;
					edges.add(value.get());
					oldValue=value.get();
				}
				((SkeletonNeighborhood) outValue).setSimpleEdges(edges);
				((LongWritable) outkey).set(key.get());
			}
			try {
				context.write(outkey, outValue);
			//	System.out.println("Key: "+outkey);
			//	System.out.println("Value: "+outValue);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		protected void setup(Context context) 
		{
			outputformat=context.getConfiguration().getInt(REQUIRED_OUTPUT_FORMAT, 0);
			switch(outputformat)
			{
				case 0:
					outkey=NullWritable.get();
					outValue=new Text();
					break;
				case 1:
					outkey=new LongWritable();
					outValue=new LongDoubleFloatNeighborhood();
					break;
				case 2:
					outkey=new LongWritable();
					outValue=new SkeletonNeighborhood();
					break;
				default:
						throw new RuntimeException("unrecognized output format: "+outputformat);
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 5) {
			System.err.println("At least 5 arguments are requiered: <InputPath> <OutputPath> <#Reducers> <UndirectedGraph> <outputFormat> <rangepartition file?>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setMapperClass(InnerMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		job.setReducerClass(InnerReducer.class);
		job.setJarByClass(StanfordReader.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.getConfiguration().setBoolean(CHANGE_TO_UNDIRECTED_GRAPH, Boolean.parseBoolean(args[3]));
		int format=Integer.parseInt(args[4]);
		job.getConfiguration().setInt(REQUIRED_OUTPUT_FORMAT, format);
		if(format==0)
		{
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
		}else if(format==1)
		{
			job.setOutputFormatClass(KVBinaryOutputFormat.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(LongDoubleFloatNeighborhood.class);
		}else if(format==2)
		{
			job.setOutputFormatClass(KVBinaryOutputFormat.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(SkeletonNeighborhood.class);
		}
		
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}

		if(args.length>=6)
		{
			String confStr=MyLongRangePartitionerFactory.readPartitionString(job.getConfiguration(), args[5]).str;
			job.getConfiguration().set(RANGE_PARTITION_BOUNDARIES, confStr);
			job.setPartitionerClass(OrderedRangePartitioner.class);
		}
		
		job.waitForCompletion(true);
		
		if(args.length>=6)
		{
			//wirte partition file
			OrderedRangePartitioner partitioner=new OrderedRangePartitioner();
			partitioner.setConf(job.getConfiguration());
			partitioner.writePartitions(args[0]+".partition", job.getNumReduceTasks());
		}
		return 0;
	}
	
	static class OrderedRangePartitioner 
	  extends Partitioner<LongWritable, LongWritable> implements Configurable{

		Configuration conf;
		long[] boundaries=null;

		private int binarySearch(long x)
		{
			 int min = 0;
			 int max =boundaries.length-1;
			 while(min<=max)
			 {
				 int mid=min+(max-min)/2;
				 if(x<boundaries[mid])
					 max=mid-1;
				 else if(x>boundaries[mid])
					 min=mid+1;
				 else
					 return mid;
			 }
			 return min;
		}
		
		public void writePartitions(String paritionFile, int numPartitions) throws IOException {
			FileSystem fs=FileSystem.get(conf);
			BufferedWriter out=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(paritionFile))));
			ArrayList<Long>[] assignment=new ArrayList[numPartitions];
			for(int i=0; i<boundaries.length; i++)
			{
				int part=i%numPartitions;
				if(assignment[part]==null)
					assignment[part]=new ArrayList<Long>();
				assignment[part].add(boundaries[i]);
			}
			int part=boundaries.length%numPartitions;
			if(assignment[part]==null)
				assignment[part]=new ArrayList<Long>();
			assignment[part].add(Long.MAX_VALUE);
			for(int i=0; i<assignment.length; i++)
			{
				out.write(Integer.toString(i));
				for(int j=0; j<assignment[i].size(); j++)
					out.write(":"+assignment[i].get(j));
				out.write("\n");
			}
			out.close();
		}

		@Override
		public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
			return binarySearch(key.get())%numPartitions;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public void setConf(Configuration cf) {
			conf=cf;
			String str=conf.get(RANGE_PARTITION_BOUNDARIES, null);
			if(str==null)
				throw new RuntimeException("partition range is empty!");
			String[] strs=str.split(",");
			boundaries=new long[strs.length];
			for(int i=0; i<strs.length; i++)
				boundaries[i]=Long.parseLong(strs[i]);
		}
	  }
	 
	
	public static void main(String[] args) throws Exception
	{
		new StanfordReader().run(args);
	}
}
