package org.apache.giraph.graph;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.ibm.giraph.graph.example.ioformats.Neighborhood;



public class LongLongLongNeighborhood<V extends Writable, E extends Writable> implements Neighborhood<LongWritable, V, E>{
	protected long[] edgeIDs;
	protected long[] edgeValues;
	protected int n;
	
	public String toString()
	{
		String str=" numEdges: "+n+"\n";
		for(int i=0; i<n; i++)
			str+=edgeIDs[i]+", " +edgeValues[i]+ ",";
		str+="\n";
		return str;
	}
	
	public void setEdges(Collection<Edge<LongWritable, LongWritable>> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Edge<LongWritable, LongWritable> e: edges)
		{
			edgeIDs[i]=e.getDestVertexId().get();
			edgeValues[i]=e.getEdgeValue().get();
			i++;
		}
	}
	
	public void setSimpleEdges(Collection<Long> edges, Collection<Long> values)
	{
		setSize(edges.size());
		int i=0;
		for(long e: edges)
		{
			edgeIDs[i]=e;
			i++;
		}
		i=0;
		for(long v: values)
		{
			edgeValues[i]=v;
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
			edgeValues[i] = 0;
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
	
	public long getEdgeValueByIndex(int i)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		return edgeValues[i];
	}
	
	public void setEdgeValueByIndex(int i,long ev)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		edgeValues[i] = ev;
	}
	
	public void setSize(int s)
	{
		n=s;
		if(edgeIDs==null || edgeIDs.length<n)
		{
			edgeIDs=new long[n];
		}
		if(edgeValues==null || edgeValues.length<n)
		{
			edgeValues=new long[n];
		}
	}
	
	public LongLongLongNeighborhood()
	{
		n=0;
		edgeIDs=null;
		edgeValues=null;
	}
	
	public LongLongLongNeighborhood(int capacity)
	{
		n=0;
		edgeIDs=new long[capacity];
		edgeValues=new long[capacity];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		setSize(in.readInt());
		for(int i=0; i<n; i++)
		{
			edgeIDs[i]=in.readLong();
			edgeValues[i]=in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(n);
		for(int i=0; i<n; i++)
		{
			out.writeLong(edgeIDs[i]);
			out.writeLong(edgeValues[i]);
		}	
	}	
}

