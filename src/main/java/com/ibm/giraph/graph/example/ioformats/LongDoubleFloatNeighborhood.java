package com.ibm.giraph.graph.example.ioformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class LongDoubleFloatNeighborhood implements Neighborhood<LongWritable, DoubleWritable, FloatWritable>
{
	private double vertexValue;
	private long[] edgeIDs;
	private float[] edgeValues;
	private int n;
	
	public String toString()
	{
		String str="vertexValue: "+vertexValue+" numEdges: "+n+"\n";
		for(int i=0; i<n; i++)
			str+=edgeIDs[i]+": "+edgeValues[i]+", ";
		str+="\n";
		return str;
	}
	
	public void setVertexValue(double v)
	{
		vertexValue=v;
	}
	
	public void setEdges(Collection<Edge<LongWritable, FloatWritable>> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Edge<LongWritable, FloatWritable> e: edges)
		{
			edgeIDs[i]=e.getDestVertexId().get();
			edgeValues[i]=e.getEdgeValue().get();
			i++;
		}
	}
	
	public void setSimpleEdges(Collection<Long> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Long e: edges)
		{
			edgeIDs[i]=e;
			edgeValues[i]=1;
			i++;
		}
	}
	
	public void set(BasicVertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex)
	{
		vertexValue=vertex.getVertexValue().get();
		setSize(vertex.getNumOutEdges());
		int i=0;
		for(LongWritable v: vertex)
		{
			edgeIDs[i]=v.get();
			edgeValues[i]=vertex.getEdgeValue(v).get();
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
	
	public float getEdgeValue(int i)
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
			edgeValues=new float[n];
		}
	}
	
	public LongDoubleFloatNeighborhood()
	{
		n=0;
		edgeIDs=null;
		edgeValues=null;
	}
	
	public LongDoubleFloatNeighborhood(int capacity)
	{
		n=0;
		edgeIDs=new long[capacity];
		edgeValues=new float[capacity];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexValue=in.readDouble();
		setSize(in.readInt());
		for(int i=0; i<n; i++)
		{
			edgeIDs[i]=in.readLong();
			edgeValues[i]=(float)in.readDouble();//in.readFloat();
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
}
