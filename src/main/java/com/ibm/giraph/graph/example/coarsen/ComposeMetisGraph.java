package com.ibm.giraph.subgraph.example.coarsen;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;

import com.ibm.giraph.formats.binary.LongParMetisVertexValueLongMNeighborhood;

public class ComposeMetisGraph {

	public static void main(String[] args) throws IOException {
		if(args.length<2)
		{
			System.err.println("MetisReader <hdfs metis files> <local metis file>");
			System.exit(-1);
		}
		File dir=new File(args[0]);
		File[] files=dir.listFiles();
		TreeMap<String, File> ordered=new TreeMap<String, File>();
		for(File file: files)
		{
			if(file.getName().contains("part"))
				ordered.put(file.getName(), file);
		}
		
		BufferedWriter out=new BufferedWriter(new FileWriter(args[1], true));
		
		LongWritable key=new LongWritable(); 
		LongParMetisVertexValueLongMNeighborhood value=new LongParMetisVertexValueLongMNeighborhood();
		for(File file: ordered.values())
		{
			DataInputStream in = new DataInputStream (new BufferedInputStream(new FileInputStream(file)));
			while(true)
			{
				try{
					key.readFields(in);
					value.readFields(in);
					out.write(Long.toString(value.getVertexValue().value));
					for(int i=0; i<value.getNumberEdges(); i++)
						out.write(" "+value.getEdgeID(i)+" "+value.getEdgeValue(i));
					out.write("\n");
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
		out.close();
	}
}
