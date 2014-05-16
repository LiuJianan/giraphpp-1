package com.ibm.giraph.graph.example.reachability;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.LongLongLongLongVertex;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.io.LongWritable;

public class SequentialBFS {
    private void traverseFrom(
	    LongLongLongLongVertex startVertex,
	    Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph)
	    throws IOException {
	ArrayList<LongLongLongLongVertex> queue = new ArrayList<LongLongLongLongVertex>();
	queue.add(startVertex);
	long tag = startVertex.getVertexValueSimpleType();
	while (!queue.isEmpty()) {
	    LongLongLongLongVertex curNode = queue.remove(queue.size() - 1);
	    for (long neighbor : curNode.getNeighborsSimpleType()) {
		LongLongLongLongVertex nVertex = (LongLongLongLongVertex) subgraph
			.getVertex(new LongWritable(neighbor));
		long mytag = nVertex.getVertexValueSimpleType();
		if ((mytag | tag) != mytag) {
		    mytag |= tag;
		    nVertex.setVertexValueSimpleType(mytag);
		    queue.add(nVertex);
		}

	    }
	}
    }

    public void LabelSourceAndDest(
	    Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph)
	    throws IOException {

	Collection<BasicVertex<LongWritable, LongWritable, LongWritable, LongWritable>> vertices = subgraph
		.getVertices();

	Iterator<BasicVertex<LongWritable, LongWritable, LongWritable, LongWritable>> iterator = vertices
		.iterator();
	while (iterator.hasNext()) {
	    LongLongLongLongVertex vertex = (LongLongLongLongVertex) iterator
		    .next();
	    vertex.voteToHalt();
	    long tag = 0;
	    if (vertex.getVertexIdSimpleType() == ReachabilityGraph.BFS_SOURCE) {
		tag = 1;
	    } else if (vertex.getVertexIdSimpleType() == ReachabilityGraph.BFS_DEST) {
		tag = 2;
	    }
	    vertex.setVertexValue(new LongWritable(tag));
	    if (tag != 0)
		traverseFrom(vertex, subgraph);
	}
	iterator = vertices.iterator();
	while (iterator.hasNext()) {
	    LongLongLongLongVertex vertex = (LongLongLongLongVertex) iterator
		    .next();
	    long tag = vertex.getVertexValueSimpleType();
	    for (long neighbor : vertex.getNeighborsSimpleType()) {
		LongLongLongLongVertex nVertex = (LongLongLongLongVertex) subgraph
			.getVertex(new LongWritable(neighbor));
		if (nVertex == null) {
		    vertex.sendMsg(new LongWritable(neighbor),
			    new LongWritable(tag));
		}
	    }
	}

    }

}
