package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KVBinaryOutputFormat<NeighborhoodType extends Writable> extends FileOutputFormat<LongWritable, NeighborhoodType>
{
	public static final String OUTPUT_NEIGHBORHOOD_VALUE_CLASS="output.neighborhood.value.class";
	public static final void setOutputNeighborhoodClass(Configuration conf, Class<? extends Writable> cls)
	{
		conf.setClass(OUTPUT_NEIGHBORHOOD_VALUE_CLASS, cls, Writable.class);
	}
	public static final Class<? extends Writable> getOutputNeighborhoodClass(Configuration conf)
	{
		return (Class<? extends Writable>) conf.getClass(OUTPUT_NEIGHBORHOOD_VALUE_CLASS, Writable.class);
	}
	
	@Override
	public RecordWriter<LongWritable, NeighborhoodType> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		Path file = getDefaultWorkFile(context, "");
		Configuration conf=context.getConfiguration();
	    FileSystem fs = file.getFileSystem(conf);
		
		return new KVBinaryRecordWriter<NeighborhoodType>(fs.create(file, true, conf.getInt("io.file.buffer.size", 4096), 
	    		fs.getDefaultReplication(), fs.getDefaultBlockSize(), context));
	}
	
	public static class KVBinaryRecordWriter<NeighborhoodType extends Writable> extends RecordWriter<LongWritable, NeighborhoodType>
	{
		/** file input stream */
		private FSDataOutputStream out;

		public KVBinaryRecordWriter(FSDataOutputStream fstream)
		{
			out=fstream;
		}
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			out.close();
		}

		@Override
		public void write(LongWritable key,
				NeighborhoodType value) throws IOException,
				InterruptedException {
			key.write(out);
			value.write(out);
		}
	}
}
