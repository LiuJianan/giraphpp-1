package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.LongLongLongNeighborhood;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.Neighborhood;

public class SimpleLongLongLongVertexBinaryInputFormat <M extends Writable> 
extends VertexInputFormat<LongWritable, LongWritable, LongWritable, M>{
	
	
	protected KVBinaryInputFormat<LongLongLongNeighborhood<LongWritable,LongWritable> > inputFormat= new KVBinaryInputFormat<LongLongLongNeighborhood<LongWritable,LongWritable> >();
	public static final Class<? extends Neighborhood> NEIGHBORHOOD_CLASS=LongLongLongNeighborhood.class;
	
	public static class SimpleVertexBinaryReader<M extends Writable> 
	extends SimpleVertexBinaryReaderBase<LongWritable, LongWritable, M, LongLongLongNeighborhood<LongWritable,LongWritable> >
	{
		public SimpleVertexBinaryReader(
				RecordReader<LongWritable, LongLongLongNeighborhood<LongWritable,LongWritable> > basereader) {
			super(basereader);
		}

		@Override
		public BasicVertex<LongWritable, LongWritable, LongWritable, M> getCurrentVertex()
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
		    BasicVertex<LongWritable, LongWritable, LongWritable, M> vertex = BspUtils.createVertex(conf);

		    LongWritable vertexId = new LongWritable(reader.getCurrentKey().get());
		    LongLongLongNeighborhood<LongWritable,LongWritable> value=reader.getCurrentValue();
		    LongWritable vertexValue=new LongWritable(0);

		    int n=value.getNumberEdges();
		    Map<LongWritable, LongWritable> edges = Maps.newHashMap();
			for(int i=0; i<n; i++)
			{
				edges.put(new LongWritable(value.getEdgeID(i)), new LongWritable(value.getEdgeValueByIndex(i)));
			}

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
	public VertexReader<LongWritable, LongWritable, LongWritable, M> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new SimpleVertexBinaryReader<M>(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
