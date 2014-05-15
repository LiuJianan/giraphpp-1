package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;

import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.Neighborhood;
import com.ibm.giraph.graph.example.ioformats.SimpleLongXXXBinaryVertexOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleVertexBinaryWriterBase;


public class SimpleLongDoubleDoubleVertexBinaryOutputFormat<V extends Writable, E extends Writable, NeighborhoodType extends Neighborhood<LongWritable, V, E>> 
extends VertexOutputFormat<LongWritable, V, E>{

	private static final Logger LOG = Logger.getLogger(SimpleLongDoubleDoubleVertexBinaryOutputFormat.class);
	
	private KVBinaryOutputFormat<NeighborhoodType> outputFormat
	=new KVBinaryOutputFormat<NeighborhoodType>();
	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		outputFormat.checkOutputSpecs(context);
	}

	@Override
	public VertexWriter<LongWritable, V, E> createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new SimpleVertexBinaryWriterBase<V, E, NeighborhoodType>(outputFormat.getRecordWriter(context));
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return outputFormat.getOutputCommitter(context);
	}
}
