package com.ibm.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This is the GiraphMapper facing interface
 * Its define the compute method and the pre/post Application and Superstep methods
 * 
 * 
 * @param <I>  Vertex index value
 * @param <V>  Vertex value
 * @param <E>  Edge value
 * @param <M>  Message value
 * 
 */

@SuppressWarnings("rawtypes")
public interface SubGraph<I extends WritableComparable, V extends Writable, 
E extends Writable, M extends Writable> {
	/**
	 * Must be defined by user to do computation on a single subgraph within one
	 * superstep. dictionary contains a SortedMap of all Vertices within the
	 * VertexRange [local Vertices]
	 * 
	 * @param collection a collection of vertices to compute
	 * @param dictionary the coresponding dictionary containing the vertex-id as key and the vertex object as vallue
	 * @param contex2 the current GiraphMapper context
	 * @param sendersideCombiner the senderside Combiner to use.
	 * @throws IOException 
	 * 
	 */
	public void compute(Partition<I, V, E, M> partition, Context context2, 
			VertexCombiner<I, M> sendersideCombiner, GraphState<I, V, E, M> gstate) throws IOException;

	public long getSuperstep();
	
	//public void writeAuxiliaryDataStructure(DataOutput output) throws IOException;
	
	//public void readAuxiliaryDataStructure(DataInput input) throws IOException;
	
	public void writeAuxiliaryDataStructure(DataOutput output) throws IOException; 
	public void readAuxiliaryDataStructure(DataInput input) throws IOException;
}
