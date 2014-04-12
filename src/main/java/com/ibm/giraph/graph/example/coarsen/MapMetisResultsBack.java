package com.ibm.giraph.subgraph.example.coarsen;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ibm.giraph.formats.binary.KVBinaryOutputFormat;
import com.ibm.giraph.formats.binary.LongParMetisVertexValueLongMNeighborhood;
import com.ibm.giraph.utils.MapRedudeUtils;

public class MapMetisResultsBack {
	
	int n=0;
	int[] parts;
	
	public MapMetisResultsBack()
	{
		
	}

	public void getN(String filename) throws IOException
	{
		BufferedReader in= new BufferedReader(new FileReader(new File(filename)));
		String line=in.readLine();
		//get the number of nodes
		n=Integer.parseInt(line.split(" ")[0]);
		in.close();
	}
	
	public void readParts(String filename) throws IOException
	{
	//	System.out.println("n = "+n);
		parts=new int[n];
		BufferedReader in= new BufferedReader(new FileReader(new File(filename)));
		String line;
		int i=0;
		while( (line=in.readLine())!=null)
		{
			parts[i]=Integer.parseInt(line);
		//	System.out.println("part for "+i+" is "+parts[i]);
			i++;
		}
		in.close();
		
	}
	
	public void Remap(String oldmapfile, String newmapfile) throws IOException, InterruptedException
	{
		LongWritable key=new LongWritable();
		LongParMetisVertexValueLongMNeighborhood value=new LongParMetisVertexValueLongMNeighborhood();
		BufferedReader in= new BufferedReader(new InputStreamReader(FileSystem.get(new Job().getConfiguration()).open(new Path(oldmapfile))));
		KVBinaryOutputFormat<LongParMetisVertexValueLongMNeighborhood> outformat
		=new KVBinaryOutputFormat<LongParMetisVertexValueLongMNeighborhood>();
		
		Job job = new Job();
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongParMetisVertexValueLongMNeighborhood.class);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongParMetisVertexValueLongMNeighborhood.class);
		Path outpath=new Path(newmapfile);
		FileOutputFormat.setOutputPath(job, outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		TaskAttemptContext context=new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
		RecordWriter<LongWritable, LongParMetisVertexValueLongMNeighborhood> writer
		=outformat.getRecordWriter(context);
		
		String line;
		while( (line=in.readLine())!=null)
		{
			String[] strs=line.split(" ");
			key.set(Long.parseLong(strs[0]));
			int index=Integer.parseInt(strs[1])-1;
			value.setVertexValue((byte)2, -1-parts[index]);
			// write down the key value pair
			writer.write(key, value);
		//	System.out.println("out: "+key+" "+value);
		}
		in.close();
		writer.close(context);
	}
	
	public static void main(String args[]) throws Exception {
		if(args.length<4)
		{
			System.err.println("4 parameters <local input metis graph file> <metis partition file> <mapping file> <output file>");
			System.exit(-1);
		}
		MapMetisResultsBack mapback=new MapMetisResultsBack();
		mapback.getN(args[0]);
		mapback.readParts(args[1]);
		mapback.Remap(args[2], args[3]);
	}
}
