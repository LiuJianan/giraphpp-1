package com.ibm.giraph.graph.example.pagerank;

import java.io.IOException;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.ibm.giraph.utils.UnmodifiableSingleItem;

public class SumCombiner extends VertexCombiner<LongWritable, DoubleWritable>
{
	@Override
	public Iterable<DoubleWritable> combine(LongWritable vertexIndex,
			Iterable<DoubleWritable> messages) throws IOException {
		double sum=0;
		for (DoubleWritable w : messages)
		{
			sum+=w.get();
		}
		return (Iterable<DoubleWritable>) new UnmodifiableSingleItem<DoubleWritable>(new DoubleWritable(sum));
	}

}