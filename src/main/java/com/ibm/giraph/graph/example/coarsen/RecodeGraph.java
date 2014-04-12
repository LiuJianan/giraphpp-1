package com.ibm.giraph.subgraph.example.coarsen;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenLongLongHashMap;
import org.mortbay.log.Log;

import com.ibm.giraph.formats.binary.KVBinaryInputFormat;
import com.ibm.giraph.formats.binary.KVBinaryOutputFormat;
import com.ibm.giraph.formats.binary.SkeletonNeighborhood;

public class RecodeGraph implements Tool {

	private Configuration conf;
	public static final String MAP_FILE_NAME="map.file.name";
	public static final String NUM_NODES="num.nodes";
	static class MyMapper extends Mapper<LongWritable, SkeletonNeighborhood, 
	LongWritable, SkeletonNeighborhood>
	{
		private int[] map;
		public void setup(Context context) throws IOException, InterruptedException 
		{
			long n=context.getConfiguration().getLong(NUM_NODES, 0);
			map=new int[(int) n+1];
			JobConf conf=new JobConf(context.getConfiguration());
			try {
				long newid=0;
				Path[] files=DistributedCache.getLocalCacheFiles(conf);
				TreeMap<String, Path> ordered=new TreeMap<String, Path>();
				for(Path file: files)
				{
					ordered.put(file.getName(), file);
				}
				/////////////////
				for(Path file: ordered.values())
				{
					Log.info("load distributed cache: "+file);
					DataInputStream in = new DataInputStream (new BufferedInputStream(new FileInputStream(file.toString())));
					while(true)
					{
						try{
							long oldid=in.readLong();
							map[(int)oldid]=(int)newid;
							newid++;
						}catch (EOFException e)
						{
							break;
						}catch (IOException e)
						{
							throw e;
						}
					}
					in.close();
				}
			}catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			/*
			String file=context.getConfiguration().get(MAP_FILE_NAME);
			FSDataInputStream in=FileSystem.get(new Job().getConfiguration()).open(new Path(file));
			long newid=0;
			while(true)
			{
				try{
				long oldid=in.readLong();
				map.put(oldid, newid);
				newid++;
				}catch (EOFException e)
				{
					break;
				}catch (IOException e)
				{
					throw e;
				}
			}
			in.close();*/
		}
		
		public void map(LongWritable key, SkeletonNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
			//if(!map.containsKey((int) key.get()))
			//	throw new IOException("cannot find the mapping for node "+key.get());
			key.set(map[(int) key.get()]);
			for(int i=0; i<value.getNumberEdges(); i++)
			{
				//if(!map.containsKey((int) value.getEdgeID(i)))
				//	throw new IOException("cannot find the mapping for node "+value.getEdgeID(i));
				value.setEdgeID(i, map[((int) value.getEdgeID(i))]);
			}
			context.write(key, value);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("At least 3 arguments are requiered: <graph file> <recode mapping> <OutputPath> <#nodes>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJobName(this.getClass().getName());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(SkeletonNeighborhood.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SkeletonNeighborhood.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().set(MAP_FILE_NAME, args[1]);
		Path outpath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outpath);
		job.setJarByClass(RecodeGraph.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(SkeletonNeighborhood.class);
		
		JobConf conf=new JobConf(job.getConfiguration());
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(args[1]));
		for(FileStatus stat: stats)
			if(stat.getPath().getName().contains("part"))
				DistributedCache.addCacheFile(stat.getPath().toUri(), conf);
		job=new Job(conf);
		
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}
		job.getConfiguration().setLong(NUM_NODES, Long.parseLong(args[3]));
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
		ToolRunner.run(new RecodeGraph(), args);
	}
}
