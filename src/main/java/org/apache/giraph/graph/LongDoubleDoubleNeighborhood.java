package org.apache.giraph.graph;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.TreeMap;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.ibm.giraph.graph.example.ioformats.Neighborhood;


public class LongDoubleDoubleNeighborhood implements Neighborhood<LongWritable, DoubleWritable, DoubleWritable>
{
	protected double vertexValue;
	protected long[] edgeIDs;
	protected double[] edgeValues;
	protected int n;
	
	public String toString()
	{
		String str="vertexValue: "+vertexValue+" numEdges: "+n+"\n";
		for(int i=0; i<n; i++)
			str+=edgeIDs[i]+", " + edgeValues[i] + ",";
		str+="\n";
		return str;
	}
	
	public void setVertexValue(double v)
	{
		vertexValue=v;
	}
	
	public void setEdges(Collection<Edge<LongWritable, DoubleWritable>> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Edge<LongWritable, DoubleWritable> e: edges)
		{
			edgeIDs[i]=e.getDestVertexId().get();
			edgeValues[i] = e.getEdgeValue().get();
			i++;
		}
	}
	
	public void setSimpleEdges(long[] edges, double[] values)
	{
		setSize(edges.length);
		
		
		for(int i = 0 ;i < edges.length ; i ++)
		{
			edgeIDs[i]=edges[i];
			edgeValues[i] = values[i];
		}
	}
	
	public void set(BasicVertex<LongWritable, DoubleWritable, DoubleWritable, ?> vertex)
	{
		vertexValue=vertex.getVertexValue().get();
		setSize(vertex.getNumOutEdges());
		int i=0;
		for(LongWritable v: vertex)
		{
			edgeIDs[i]=v.get();
			edgeValues[i] = 0;
			i++;
		}
	}
	
	public double getVertexValue()
	{
		return vertexValue;
	}
	
	public int getNumberEdges()
	{
		return n;
	}
	
	public long getEdgeID(int i)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		return edgeIDs[i];
	}
	public double getEdgeValue(int i)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		return edgeValues[i];
	}
	
	private void setSize(int s)
	{
		n=s;
		if(edgeIDs==null || edgeIDs.length<n)
		{
			edgeIDs=new long[n];
		}
		if(edgeValues==null || edgeValues.length<n)
		{
			edgeValues=new double[n];
		}
	}
	
	public LongDoubleDoubleNeighborhood()
	{
		n=0;
		edgeIDs=null;
		edgeValues=null;
	}
	
	public LongDoubleDoubleNeighborhood(int capacity)
	{
		n=0;
		edgeIDs=new long[capacity];
		edgeValues=new double[capacity];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexValue=in.readDouble();
		setSize(in.readInt());
		for(int i=0; i<n; i++)
		{
			edgeIDs[i]=in.readLong();
			edgeValues[i] = in.readDouble();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(vertexValue);
		out.writeInt(n);
		for(int i=0; i<n; i++)
		{
			out.writeLong(edgeIDs[i]);
			out.writeDouble(edgeValues[i]);
		}	
	}
	
	public static void main(String[] args) throws IOException
	{
		
		File dir=new File("out.pr");
		File[] files=dir.listFiles();
		TreeMap<String, File> ordered=new TreeMap<String, File>();
		for(File file: files)
		{
			if(file.getName().contains("part"))
				ordered.put(file.getName(), file);
		}
		LongDoubleDoubleNeighborhood value=new LongDoubleDoubleNeighborhood();
		LongWritable key=new LongWritable();
		for(File file: ordered.values())
		{
			DataInputStream in = new DataInputStream (new BufferedInputStream(new FileInputStream(file)));
			while(true)
			{
				try{
					key.readFields(in);
					value.readFields(in);
					System.out.println("key: "+key+", value: "+value);
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
	}
}
