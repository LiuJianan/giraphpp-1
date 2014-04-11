package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class SimpleVertexBinaryReaderBase<V extends Writable, E extends Writable, 
M extends Writable, NeightborhoodType extends Writable> 
implements VertexReader<LongWritable, V, E, M>{

	protected RecordReader<LongWritable, NeightborhoodType> reader;
	/** Context passed to initialize */
    protected TaskAttemptContext context;
	
	public SimpleVertexBinaryReaderBase(RecordReader<LongWritable, NeightborhoodType> basereader)
	{
		reader=basereader;
	}
	
	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader.initialize(inputSplit, context);
		this.context=context;
	}

	@Override
	public boolean nextVertex() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}

	@Override
	abstract public BasicVertex<LongWritable, V, E, M> getCurrentVertex()
			throws IOException, InterruptedException;
}
