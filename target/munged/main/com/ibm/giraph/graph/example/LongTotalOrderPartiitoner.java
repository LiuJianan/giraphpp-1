package com.ibm.giraph.graph.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

//key needs to be consequtive numbers from 0 to n-1
public class LongTotalOrderPartiitoner<V extends Writable> extends Partitioner<LongWritable, V> implements Configurable{

	public static final String NUM_KEYS="num.keys";
	private long partitionSize=0;
	//private int numReducers=0;
	private Configuration conf;
	@Override
	public int getPartition(LongWritable key, V value, int numPartitions) {
		return (int) (key.get()/partitionSize);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf=conf;
		long n=conf.getLong(NUM_KEYS, 0);
		try {
			Job job = new Job(conf);
			int numReducers=job.getNumReduceTasks();
			partitionSize=(long) Math.ceil((double)n/(double)numReducers);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public static void setNumKeys(Configuration conf, long maxkey)
	{
		conf.setLong(NUM_KEYS, maxkey);
	}

}
