package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;
import com.ibm.giraph.graph.example.wcc.SequentialWCC;

public class SimpleLongLongNullXBinaryVertexInputFormat <M extends Writable> 
extends VertexInputFormat<LongWritable, LongWritable, NullWritable, M>{

	protected KVBinaryInputFormat<SkeletonNeighborhood<LongWritable, NullWritable>> inputFormat= new KVBinaryInputFormat<SkeletonNeighborhood<LongWritable, NullWritable>>(); 
	public static final Class<? extends Neighborhood> NEIGHBORHOOD_CLASS=SkeletonNeighborhood.class;
	
	public static class SimpleVertexBinaryReader<M extends Writable> 
	extends SimpleVertexBinaryReaderBase<LongWritable, NullWritable, M, SkeletonNeighborhood<LongWritable, NullWritable>>
	{
		public SimpleVertexBinaryReader(
				RecordReader<LongWritable, SkeletonNeighborhood<LongWritable, NullWritable>> basereader) {
			super(basereader);
		}

		@Override
		public BasicVertex<LongWritable, LongWritable, NullWritable, M> getCurrentVertex()
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
		    BasicVertex<LongWritable, LongWritable, NullWritable, M> vertex = BspUtils.createVertex(conf);

		    LongWritable vertexId = new LongWritable(reader.getCurrentKey().get());
		    SkeletonNeighborhood<LongWritable, NullWritable> value=reader.getCurrentValue();
		    LongWritable vertexValue=new LongWritable(SequentialWCC.DEFAULT);
		    
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
	public VertexReader<LongWritable, LongWritable, NullWritable, M> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new SimpleVertexBinaryReader<M>(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
