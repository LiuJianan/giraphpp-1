package com.ibm.giraph.graph.example.ioformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.LongWritable;
import org.mortbay.log.Log;

import com.ibm.giraph.graph.example.ioformats.Neighborhood;

import com.ibm.giraph.graph.example.coarsen.CoarsenVertexValue;

public class LongCoarsenVertexValueLongMNeighborhood implements Neighborhood<LongWritable, CoarsenVertexValue, LongWritable>
{
	private CoarsenVertexValue vertexValue=new CoarsenVertexValue();
	private long[] edgeIDs;
	private long[] edgeValues;
	private int n;
	
	public LongCoarsenVertexValueLongMNeighborhood(LongCoarsenVertexValueLongMNeighborhood that)
	{
		this.vertexValue.set(that.getVertexValue());
		this.n=that.n;
		edgeIDs=new long[n];
		edgeValues=new long[n];
		for(int i=0; i<n; i++)
		{
			this.edgeIDs[i]=that.edgeIDs[i];
			this.edgeValues[i]=that.edgeValues[i];
		}
	}
	
	public String toString()
	{
		String str="vertexValue: "+vertexValue+" numEdges: "+n+"\n";
		for(int i=0; i<n; i++)
			str+=edgeIDs[i]+": "+edgeValues[i]+", ";
		str+="\n";
		return str;
	}
	
	public void setVertexValue(byte state, long value)
	{
		vertexValue.set(state, value);
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
	
	public void set(BasicVertex<LongWritable, CoarsenVertexValue, LongWritable, ?> vertex)
	{
		//Log.info("---- set vertex: "+vertex);
		vertexValue.set(vertex.getVertexValue());
		setSize(vertex.getNumOutEdges());
		int i=0;
		for(LongWritable v: vertex)
		{
			edgeIDs[i]=v.get();
			edgeValues[i]=vertex.getEdgeValue(v).get();
			i++;
		}
	}
	
	public CoarsenVertexValue getVertexValue()
	{
		return vertexValue;
	}
	
	public int getNumberEdges()
	{
		return n;
	}
	
	public void setEdgeID(int i, long id)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		edgeIDs[i]=id;
	}
	
	
	public long getEdgeID(int i)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		return edgeIDs[i];
	}
	
	public long getEdgeValue(int i)
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
			edgeValues=new long[n];
		}
	}
	
	public LongCoarsenVertexValueLongMNeighborhood()
	{
		n=0;
		edgeIDs=null;
		edgeValues=null;
	}
	
	public LongCoarsenVertexValueLongMNeighborhood(int capacity)
	{
		n=0;
		edgeIDs=new long[capacity];
		edgeValues=new long[capacity];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexValue.readFields(in);
		setSize(in.readInt());
		for(int i=0; i<n; i++)
		{
			edgeIDs[i]=in.readLong();
			edgeValues[i]=in.readLong();//in.readFloat();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		vertexValue.write(out);
		out.writeInt(n);
		for(int i=0; i<n; i++)
		{
			out.writeLong(edgeIDs[i]);
			out.writeLong(edgeValues[i]);
		}	
	}	
}

