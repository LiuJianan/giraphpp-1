package com.ibm.giraph.subgraph.example.coarsen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.map.OpenLongIntHashMap;
import org.mortbay.log.Log;

import com.ibm.giraph.formats.binary.KVBinaryInputFormat;
import com.ibm.giraph.formats.binary.KVBinaryOutputFormat;
import com.ibm.giraph.formats.binary.LongLongNullNeighborhood;
import com.ibm.giraph.formats.binary.LongParMetisVertexValueLongMNeighborhood;
import com.ibm.giraph.subgraph.example.LongTotalOrderPartiitoner;
import com.ibm.giraph.utils.MapRedudeUtils;

public class PrepareMetisInputGraph implements Tool {

	private Configuration conf;
	
	protected static enum Counters {NUM_NODES, NUM_EDGES };
	
	static class MyMapper extends Mapper<LongWritable, LongParMetisVertexValueLongMNeighborhood, LongWritable, LongParMetisVertexValueLongMNeighborhood>
	{
		long orphanNodes=0;
		OpenLongIntHashMap map=new OpenLongIntHashMap();
		protected void map(LongWritable key, LongParMetisVertexValueLongMNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
			if(value.getVertexValue().state==2 || (value.getVertexValue().value==1 && value.getNumberEdges()==0))
			{
			//	Log.info("mapper ignore: "+key);
				return;
			}
			else
			{
				if(!map.containsKey(key.get()))
					throw new RuntimeException("vertex "+key+" is not in map!!!!");
				key.set(map.get(key.get()));
				for(int i=0; i<value.getNumberEdges(); i++)
				{
					if(!map.containsKey(value.getEdgeID(i)))
						throw new RuntimeException("vertex "+key+": "+value+" has "+value.getEdgeID(i)+" not in map!!!!");
					value.setEdgeID(i, map.get(value.getEdgeID(i)));
				}
				context.write(key, value);
			}
		//	Log.info("mapper: "+value);
		}
		
		public void setup(Context context)
		{
			JobConf conf=new JobConf(context.getConfiguration());
			try {
				Path[] files=DistributedCache.getLocalCacheFiles(conf);
				for(Path file: files)
				{
					BufferedReader in = new BufferedReader(new FileReader(file.toString()));
					String line;
					while( (line=in.readLine())!=null )
					{
						String[] strs=line.split(" ");
						map.put(Long.parseLong(strs[0]), Integer.parseInt(strs[1]));
					}
					in.close();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, LongParMetisVertexValueLongMNeighborhood, LongWritable, LongParMetisVertexValueLongMNeighborhood>
	{
		
	//	Text textbuff=new Text();
		int numNodes=0; 
		int numEdges=0;
	
		public void reduce(LongWritable key, Iterable<LongParMetisVertexValueLongMNeighborhood> values, Context context) 
		throws IOException, InterruptedException 
		{
			numNodes++;
			LongParMetisVertexValueLongMNeighborhood value=values.iterator().next();
				
			context.write(key, value);
			numEdges+=value.getNumberEdges();
			if(values.iterator().hasNext())
				throw new IOException("multiple nodes have the same id: "+key);
		//	Log.info("reducer: "+nbhd);
		}
		
		public void cleanup(Context context)
		throws IOException, InterruptedException 
		{
			super.cleanup(context);
			context.getCounter(Counters.NUM_EDGES).increment(numEdges);
			context.getCounter(Counters.NUM_NODES).increment(numNodes);
		}
	}

	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 5) {
			throw new IllegalArgumentException(
					"At least 2 arguments are requiered: <input> <mapping> <output> <local metis> <# reducers>");
		}
		Job job = new Job();
		job.setJobName(this.getClass().getName());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongParMetisVertexValueLongMNeighborhood.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		Path outpath=new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongParMetisVertexValueLongMNeighborhood.class);
		//job.setReducerClass(MyReducer.class);
		job.setJarByClass(PrepareMetisInputGraph.class);
		JobConf conf=new JobConf(job.getConfiguration());
		DistributedCache.addCacheFile(new URI(args[1]+"/part-r-00000"), conf);
		
		job=new Job(conf);
		
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongParMetisVertexValueLongMNeighborhood.class);
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongParMetisVertexValueLongMNeighborhood.class);
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		job.setPartitionerClass(LongTotalOrderPartiitoner.class);
		File f=new File(args[3]);
		BufferedReader in= new BufferedReader(new FileReader(f));
		String[] strs=in.readLine().split(" ");
		LongTotalOrderPartiitoner.setNumKeys(job.getConfiguration(), Long.parseLong(strs[0]));
		in.close();
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
		ToolRunner.run(new PrepareMetisInputGraph(), args);
	}
}
