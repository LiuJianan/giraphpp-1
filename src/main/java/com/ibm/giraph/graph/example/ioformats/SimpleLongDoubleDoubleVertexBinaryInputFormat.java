package com.ibm.giraph.graph.example.ioformats;


import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.Neighborhood;

import org.apache.giraph.graph.LongDoubleDoubleNeighborhood;

public class SimpleLongDoubleDoubleVertexBinaryInputFormat <M extends Writable> 
extends VertexInputFormat<LongWritable, DoubleWritable, DoubleWritable, M>{
	
	public static final Class<? extends Neighborhood> NEIGHBORHOOD_CLASS=LongDoubleDoubleNeighborhood.class;
	
	protected KVBinaryInputFormat<LongDoubleDoubleNeighborhood> inputFormat= new KVBinaryInputFormat<LongDoubleDoubleNeighborhood>(); 
	
	public static class SimpleVertexBinaryReader<M extends Writable> 
	extends SimpleVertexBinaryReaderBase<DoubleWritable, DoubleWritable, M, LongDoubleDoubleNeighborhood>
	{
		public SimpleVertexBinaryReader(
				RecordReader<LongWritable, LongDoubleDoubleNeighborhood> basereader) {
			super(basereader);
		}

		@Override
		public BasicVertex<LongWritable, DoubleWritable, DoubleWritable, M> getCurrentVertex()
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
		    BasicVertex<LongWritable, DoubleWritable, DoubleWritable, M> vertex = BspUtils.createVertex(conf);

		    LongWritable vertexId = new LongWritable(reader.getCurrentKey().get());
		    LongDoubleDoubleNeighborhood value=reader.getCurrentValue();
		    DoubleWritable vertexValue=new DoubleWritable();
		    
		    int n=value.getNumberEdges();
		    Map<LongWritable, DoubleWritable> edges = Maps.newHashMap();
			for(int i=0; i<n; i++)
				edges.put(new LongWritable(value.getEdgeID(i)), 
						new DoubleWritable(value.getEdgeValue(i)));
		    vertex.initialize(vertexId, vertexValue, edges, null);
		    return vertex;
		}
		
	}

	@Override
	public List<InputSplit> getSplits(JobContext context, int numWorkers)
			throws IOException, InterruptedException {
		return inputFormat.getSplits(context);
	}

	@Override
	public VertexReader<LongWritable, DoubleWritable, DoubleWritable, M> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new SimpleVertexBinaryReader<M>(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
