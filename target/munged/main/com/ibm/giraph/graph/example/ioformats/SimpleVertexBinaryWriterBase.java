package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.Neighborhood;

public class SimpleVertexBinaryWriterBase<V extends Writable, E extends Writable, 
NeightborhoodType extends Neighborhood<LongWritable, V, E>> 
implements VertexWriter<LongWritable, V, E>
{
	protected RecordWriter<LongWritable, NeightborhoodType> writer;
	protected NeightborhoodType valueBuff;
	protected Class<? extends Writable> valueClass;
	protected Configuration conf;
	
	public SimpleVertexBinaryWriterBase(RecordWriter<LongWritable, NeightborhoodType> baseWriter)
	{
		writer=baseWriter;
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		writer.close(context);
	}

	@Override
	public void initialize(TaskAttemptContext context) throws IOException {	
		conf=context.getConfiguration();
		valueClass=KVBinaryOutputFormat.getOutputNeighborhoodClass(conf);
		valueBuff= (NeightborhoodType) ReflectionUtils.newInstance(valueClass, conf);
	}

	@Override
	public void writeVertex(
			BasicVertex<LongWritable, V, E, ?> vertex)throws IOException, InterruptedException
	{	
		valueBuff.set(vertex);
		writer.write(vertex.getVertexId(), valueBuff);
		//Log.info("~~~ output: "+ vertex.getVertexId()+" "+valueBuff);
	}
}
