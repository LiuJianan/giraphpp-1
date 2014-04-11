package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.LongDoubleNullDoubleVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class SimpleLongDoubleNullDoubleBinaryInputFormat extends SimpleLongDoubleNullXVertexBinaryInputFormat<DoubleWritable>{

	static class VertexBinaryReader extends SimpleLongDoubleNullXVertexBinaryInputFormat.SimpleVertexBinaryReader<DoubleWritable>
	{

		public VertexBinaryReader(
				RecordReader<LongWritable, SkeletonNeighborhood> basereader) {
			super(basereader);
		}
		@Override
		public BasicVertex<LongWritable, DoubleWritable, NullWritable, DoubleWritable> getCurrentVertex()
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
		    BasicVertex<LongWritable, DoubleWritable, NullWritable, DoubleWritable> tempV = BspUtils.createVertex(conf);
		    LongDoubleNullDoubleVertex vertex= (LongDoubleNullDoubleVertex) tempV;

		    SkeletonNeighborhood<LongWritable, NullWritable> edges=reader.getCurrentValue();
		   
		    vertex.initialize(reader.getCurrentKey().get(), 0, edges, null);
		    return vertex;
		}
	}
	
	@Override
	public VertexReader<LongWritable, DoubleWritable, NullWritable, DoubleWritable> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new VertexBinaryReader(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
