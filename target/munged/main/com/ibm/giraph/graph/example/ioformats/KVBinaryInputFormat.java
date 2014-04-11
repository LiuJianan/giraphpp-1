package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class KVBinaryInputFormat<NeighborhoodType extends Writable> extends FileInputFormat<LongWritable, NeighborhoodType>
{
	private static final Logger LOG = Logger.getLogger(KVBinaryInputFormat.class);
	public static final String INPUT_NEIGHBORHOOD_VALUE_CLASS="input.neighborhood.value.class";
	public static final void setInputNeighborhoodClass(Configuration conf, Class<? extends Writable> cls)
	{
		conf.setClass(INPUT_NEIGHBORHOOD_VALUE_CLASS, cls, Writable.class);
	}
	public static final Class<? extends Writable> getInputNeighborhoodClass(Configuration conf)
	{
		return (Class<? extends Writable>) conf.getClass(INPUT_NEIGHBORHOOD_VALUE_CLASS, Writable.class);
	}
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	@Override
	public RecordReader<LongWritable, NeighborhoodType> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new KVBinaryRecordReader<NeighborhoodType>();
	}
	
	public static class KVBinaryRecordReader<NeighborhoodType>
	extends RecordReader<LongWritable, NeighborhoodType>
	{
		/** file input stream */
		private FSDataInputStream in;
		
		/** start offset in file, inclusive */
		private long start;
		
		/** end offset in file, exclusive */
		private long end; 
		
		/** file size */
		private long fileSize;
		
		private Class<? extends Writable> valueClass;
		private Configuration conf;
		
		private LongWritable keyBuff=new LongWritable();
		private NeighborhoodType valueBuff;
		
		@Override
		public void close() throws IOException {
			in.close();				
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return keyBuff;
		}

		@Override
		public NeighborhoodType getCurrentValue() throws IOException,
				InterruptedException {
			return valueBuff;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return Math.min(1.0f, (float)((in.getPos() - start) /
	                (double)(end - start)));
		}

		@Override
		public void initialize(InputSplit rawsplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
			FileSplit split=(FileSplit)rawsplit;
			
			start = split.getStart();
			end = split.getStart() + split.getLength();
			
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			fileSize = fs.getFileStatus(path).getLen();
			if (end > fileSize) {
				throw new IllegalArgumentException("File length insufficient " 
						+ "(name=" + path.getName() + ", size=" + fs.getFileStatus(path).getLen() 
						+ ", start=" + start + ", end=" + end + ")");
			} 

			// open file
			in = fs.open(path, context.getConfiguration().getInt("io.file.buffer.size", 4096));
			in.seek(start);
			conf=context.getConfiguration();
			valueClass=getInputNeighborhoodClass(conf);
			valueBuff= (NeighborhoodType) ReflectionUtils.newInstance(valueClass, conf);
			LOG.info("valueClass: "+valueClass);
		}

		@Override
		public boolean nextKeyValue() throws IOException,
				InterruptedException {
			if (in.getPos() < end) {
				keyBuff.readFields(in);
				((Writable)valueBuff).readFields(in);
				return true;
			} else {
				return false;
			}
		}		
	}
}
