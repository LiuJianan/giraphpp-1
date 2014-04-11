package com.ibm.giraph.graph.example.coarsen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CoarsenMessageWritable implements Writable{

	public byte type=0;//0:request match, 1: merge, 2: replace edge, 3: match acknowledgement
	public long[] msg=null;
	
	public CoarsenMessageWritable(){}
	
	public CoarsenMessageWritable(byte t, long[] m)
	{
		type=t;
		msg=m;
	}
	
	public CoarsenMessageWritable(byte t, long m)
	{
		type=t;
		msg=new long[]{m};
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type=in.readByte();
		int n=in.readInt();
		msg=new long[n];
		for(int i=0; i<n; i++)
			msg[i]=in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type);
		if(msg==null)
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(msg.length);
		for(int i=0; i<msg.length; i++)
			out.writeLong(msg[i]);
	}

	public String toString()
	{
		String ret="type: "+type+" (";
		switch(type)
		{
			case 0: ret+="match request";
			break;
			case 1: ret+="match acknowledgement";
			break;
			case 2: ret+="edge replacement";
			break;
		}
		ret+=")\n[";
		for(long e: msg)
			ret+=e+", ";
		ret+="]";
		return ret;
	}
}
