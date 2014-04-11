package com.ibm.giraph.graph.example.ioformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.ibm.giraph.graph.example.ioformats.Neighborhood;

public class SkeletonNeighborhood<V extends Writable, E extends Writable> implements Neighborhood<LongWritable, V, E>{
	protected long[] edgeIDs;
	protected int n;
	
	public String toString()
	{
		String str=" numEdges: "+n+"\n";
		for(int i=0; i<n; i++)
			str+=edgeIDs[i]+", ";
		str+="\n";
		return str;
	}
	
	public void setEdges(Collection<Edge<LongWritable, NullWritable>> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Edge<LongWritable, NullWritable> e: edges)
		{
			edgeIDs[i]=e.getDestVertexId().get();
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
			i++;
		}
	}
	
	public void set(BasicVertex<LongWritable, V, E, ?> vertex)
	{
		setSize(vertex.getNumOutEdges());
		int i=0;
		for(LongWritable v: vertex)
		{
			edgeIDs[i]=v.get();
			i++;
		}
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
	
	public void setEdgeID(int i, long ev)
	{
		if((i>=n && n>0)|| i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		edgeIDs[i]=ev;
	}
	
	public void setSize(int s)
	{
		n=s;
		if(edgeIDs==null || edgeIDs.length<n)
		{
			edgeIDs=new long[n];
		}
	}
	
	public SkeletonNeighborhood()
	{
		n=0;
		edgeIDs=null;
	}
	
	public SkeletonNeighborhood(int capacity)
	{
		n=0;
		edgeIDs=new long[capacity];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		setSize(in.readInt());
		for(int i=0; i<n; i++)
		{
			edgeIDs[i]=in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(n);
		for(int i=0; i<n; i++)
		{
			out.writeLong(edgeIDs[i]);
		}	
	}	
}
