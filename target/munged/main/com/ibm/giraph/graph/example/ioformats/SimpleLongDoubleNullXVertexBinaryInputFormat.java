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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

public class SimpleLongDoubleNullXVertexBinaryInputFormat <M extends Writable> 
extends VertexInputFormat<LongWritable, DoubleWritable, NullWritable, M>{
	
	public static final Class<? extends Neighborhood> NEIGHBORHOOD_CLASS=SkeletonNeighborhood.class;
	
	protected KVBinaryInputFormat<SkeletonNeighborhood> inputFormat= new KVBinaryInputFormat<SkeletonNeighborhood>(); 
	
	public static class SimpleVertexBinaryReader<M extends Writable> 
	extends SimpleVertexBinaryReaderBase<DoubleWritable, NullWritable, M, SkeletonNeighborhood>
	{
		public SimpleVertexBinaryReader(
				RecordReader<LongWritable, SkeletonNeighborhood> basereader) {
			super(basereader);
		}

		@Override
		public BasicVertex<LongWritable, DoubleWritable, NullWritable, M> getCurrentVertex()
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
		    BasicVertex<LongWritable, DoubleWritable, NullWritable, M> vertex = BspUtils.createVertex(conf);

		    LongWritable vertexId = new LongWritable(reader.getCurrentKey().get());
		    SkeletonNeighborhood value=reader.getCurrentValue();
		    DoubleWritable vertexValue=new DoubleWritable();
		    
		    int n=value.getNumberEdges();
		    Map<LongWritable, NullWritable> edges = Maps.newHashMap();
			for(int i=0; i<n; i++)
				edges.put(new LongWritable(value.getEdgeID(i)), 
						NullWritable.get());
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
	public VertexReader<LongWritable, DoubleWritable, NullWritable, M> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new SimpleVertexBinaryReader<M>(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
