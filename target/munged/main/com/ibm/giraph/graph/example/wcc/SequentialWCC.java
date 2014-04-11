package com.ibm.giraph.graph.example.wcc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.LongLongNullLongVertex;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class SequentialWCC {

	private ArrayList<LongLongNullLongVertex> innerVertices=null;
	private LongWritable longbuff=new LongWritable();
	static final long MARK=Long.MAX_VALUE;
	public static final long DEFAULT=Long.MIN_VALUE;
	ArrayList<LongLongNullLongVertex> queue=null;
	
	private void traverseFrom(LongLongNullLongVertex startVertex, 
			Partition<LongWritable, LongWritable, NullWritable, LongWritable> subgraph
			) throws IOException
	{
		queue.clear();
		startVertex.setVertexValueSimpleType(MARK);
		long newLabel=startVertex.getVertexIdSimpleType();
		queue.add(startVertex);
		LongLongNullLongVertex curNode;
		while(!queue.isEmpty())
		{
			curNode=queue.remove(queue.size()-1);
			innerVertices.add(curNode);
			
			for(long neighbor: curNode.getNeighborsSimpleType())
			{
				if(newLabel>neighbor)
					newLabel=neighbor;
				longbuff.set(neighbor);
				LongLongNullLongVertex nVertex=(LongLongNullLongVertex) subgraph.getVertex(longbuff);
				if(nVertex!=null)
				{
					if(nVertex.getVertexValueSimpleType()!=MARK)
					{
						nVertex.setVertexValueSimpleType(MARK);
						queue.add(nVertex);
					}
				}
			}
		}
		changeLabelsForAComponent(subgraph, startVertex, newLabel, innerVertices);
		innerVertices.clear();
	}
	
	private void changeLabelsForAComponent(Partition<LongWritable, LongWritable, NullWritable, LongWritable> subgraph, 
			final LongLongNullLongVertex startVertex, final long label, ArrayList<LongLongNullLongVertex> innerVertices)
	{
		final LongWritable msg=new LongWritable(label);
		for(int i=0; i<innerVertices.size(); i++)
		{
			LongLongNullLongVertex curNode=innerVertices.get(i);
			curNode.setVertexValueSimpleType(label);
			for(long neighbor: curNode.getNeighborsSimpleType())
			{
				longbuff.set(neighbor);
				LongLongNullLongVertex nVertex=(LongLongNullLongVertex) subgraph.getVertex(longbuff);
				if(nVertex==null)
				{
					curNode.sendMsg(new LongWritable(neighbor), msg);
				}
			}

		}
		
	}
	
	public void computeWCC(Partition<LongWritable, LongWritable, NullWritable, 
			LongWritable> subgraph) 
	throws IOException
	{
		Collection<BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable>> vertices = subgraph.getVertices();
		if(innerVertices==null)
			innerVertices=new ArrayList<LongLongNullLongVertex>(vertices.size());
		if(queue==null)
			queue=new ArrayList<LongLongNullLongVertex>(vertices.size());
			
		Iterator<BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable>> 
		iterator=vertices.iterator();
		while (iterator.hasNext()) {
			LongLongNullLongVertex vertex = (LongLongNullLongVertex) iterator.next();
			vertex.voteToHalt();
			if(vertex.getNumOutEdges()==0)
			{
				vertex.setVertexValueSimpleType(vertex.getVertexIdSimpleType());
				//numNodesTraversed++;
				continue;
			}
			if(vertex.getVertexValueSimpleType()!=DEFAULT) //has been visited before
				continue;
			traverseFrom(vertex, subgraph);
		}
	}
	
	public void reset()
	{
		if(innerVertices!=null)
			innerVertices.clear();
		if(queue!=null)
			queue.clear();
	}
}
