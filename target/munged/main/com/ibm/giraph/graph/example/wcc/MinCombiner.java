package com.ibm.giraph.graph.example.wcc;

import java.io.IOException;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.giraph.utils.UnmodifiableSingleItem;

public class MinCombiner<I extends WritableComparable, M extends WritableComparable> 
extends VertexCombiner<I, M>
{
	@Override
	public Iterable<M> combine(I vertexIndex,
			Iterable<M> messages) throws IOException {
		M min = null;
		for (M m : messages)
		{
			if(min==null)
				min=m;
			else if (m.compareTo(min)<0)
				min = m;
		}
		
		return (Iterable<M>) new UnmodifiableSingleItem<M>(min);
	}
}

