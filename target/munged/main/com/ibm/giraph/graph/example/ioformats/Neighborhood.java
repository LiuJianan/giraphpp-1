package com.ibm.giraph.graph.example.ioformats;

import org.apache.giraph.graph.BasicVertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface Neighborhood<I extends WritableComparable, V extends Writable, E extends Writable> extends Writable{

	public void set(BasicVertex<I, V, E, ?> vertex);
}
