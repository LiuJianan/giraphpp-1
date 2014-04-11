package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.LongLongNullLongVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ibm.giraph.graph.example.wcc.SequentialWCC;

public class SimpleLongLongNullLongBinaryInputFormat extends SimpleLongLongNullXBinaryVertexInputFormat<LongWritable>{

	static class VertexBinaryReader extends SimpleLongLongNullXBinaryVertexInputFormat.SimpleVertexBinaryReader<LongWritable>
	{

		public VertexBinaryReader(
				RecordReader<LongWritable, SkeletonNeighborhood<LongWritable, NullWritable>> basereader) {
			super(basereader);
		}
		
		@Override
		public BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable> getCurrentVertex()
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable> tempV = BspUtils.createVertex(conf);
		    LongLongNullLongVertex vertex = (LongLongNullLongVertex) tempV;
		    SkeletonNeighborhood<LongWritable, NullWritable> edges=reader.getCurrentValue();
		    vertex.initialize(reader.getCurrentKey().get(), SequentialWCC.DEFAULT, edges, null);
		    return vertex;
		}	
	}
	
	@Override
	public VertexReader<LongWritable, LongWritable, NullWritable, LongWritable> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new VertexBinaryReader(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
