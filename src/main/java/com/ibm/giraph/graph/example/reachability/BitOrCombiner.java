
package com.ibm.giraph.graph.example.reachability;

import java.io.IOException;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.LongWritable;

import com.ibm.giraph.utils.UnmodifiableSingleItem;

public class BitOrCombiner extends VertexCombiner<LongWritable, LongWritable>
{
	@Override
	public Iterable<LongWritable> combine(LongWritable vertexIndex,
			Iterable<LongWritable> messages) throws IOException {
		long sum=0;
		for (LongWritable w : messages)
		{
			sum|=w.get();
		}
		return (Iterable<LongWritable>) new UnmodifiableSingleItem<LongWritable>(new LongWritable(sum));
	}

}