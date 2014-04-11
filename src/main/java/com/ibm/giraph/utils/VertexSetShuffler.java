package com.ibm.giraph.utils;

import java.util.Collection;
import java.util.Random;

import org.apache.giraph.graph.BasicVertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VertexSetShuffler<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> {
	public BasicVertex<I, V, E, M>[] shuffle(Collection<BasicVertex<I, V, E, M>> vertices, Random rand)
	{
		BasicVertex<I, V, E, M>[] list=new BasicVertex[vertices.size()];
		int i=0;
		for(BasicVertex<I, V, E, M> v: vertices)
		{
			list[i]=v;
			i++;
		}
		
		BasicVertex<I, V, E, M> temp;
		int j;
		for(i=list.length-1; i>0; i--)
		{
			j=rand.nextInt(i+1);
			//swap
			temp=list[i];
			list[i]=list[j];
			list[j]=temp;
		}
		return list;
	}
}
