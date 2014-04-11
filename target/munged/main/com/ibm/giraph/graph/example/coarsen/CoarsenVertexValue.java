package com.ibm.giraph.graph.example.coarsen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CoarsenVertexValue implements Writable{
	
	public byte state=1;//1: alive, 2: merged, 3: match request sent;
	public long value=1;
	
	public String toString()
	{
		return state+": "+value;
	}
	
	public CoarsenVertexValue()
	{
		
	}
	
	public CoarsenVertexValue(byte  s, long v)
	{
		set(s, v);
	}
	
	public void set(CoarsenVertexValue that)
	{
		set(that.state, that.value);
	}
	
	public void set(byte s, long v)
	{
		state=s;
		value=v;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		state=in.readByte();
		value=in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(state);
		out.writeLong(value);
	}
	
}
