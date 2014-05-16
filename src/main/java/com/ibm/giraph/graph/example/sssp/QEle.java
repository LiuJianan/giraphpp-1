package com.ibm.giraph.graph.example.sssp;

import org.apache.giraph.graph.LongDoubleDoubleDoubleVertex;

public class QEle implements Comparable<QEle>{
	
	public int id;
	public LongDoubleDoubleDoubleVertex key;
	int pos;
	
	public QEle(int id, LongDoubleDoubleDoubleVertex key)
	{
		this.id=id;
		this.key=key;
	}
	
	public int compareTo(QEle o)
	{
		if(key.getVertexValueSimpleType()>o.key.getVertexValueSimpleType()) return 1;
		else if(key.getVertexValueSimpleType()<o.key.getVertexValueSimpleType()) return -1;
		return 0;
	}
	
}