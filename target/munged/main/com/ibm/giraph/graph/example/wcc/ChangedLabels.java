package com.ibm.giraph.graph.example.wcc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.function.LongLongProcedure;
import org.apache.mahout.math.map.OpenLongLongHashMap;

public class ChangedLabels implements Writable
{
	public OpenLongLongHashMap labels=new OpenLongLongHashMap();
	public void consolidate()
	{
		labels.forEachPair(new LongLongProcedure()
		{
			@Override
			public boolean apply(long key, long value) {
				labels.put(key, find(value));
				return true;
			}
			
		});
	}
	public long find(long oldLabel)
	{
		long newLabel=oldLabel;
		while(labels.containsKey(newLabel))
		{
			newLabel=labels.get(newLabel);
		}
		return newLabel;	
	}
	public String toString()
	{
		return labels.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int n=in.readInt();
		for(int i=0; i<n; i++)
		{
			long key=in.readLong();
			long value=in.readLong();
			labels.put(key, value);
		}
	}
	
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(labels.size());
		labels.forEachPair(new LongLongProcedure(){
			@Override
			public boolean apply(long key, long value)  {
				try {
					out.writeLong(key);
					out.writeLong(value);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return true;
			}
			
		});
	}
}
