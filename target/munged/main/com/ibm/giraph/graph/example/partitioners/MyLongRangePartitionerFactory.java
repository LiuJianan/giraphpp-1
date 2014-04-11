package com.ibm.giraph.graph.example.partitioners;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.partition.MasterGraphPartitioner;
import org.apache.giraph.graph.partition.RangePartitionerFactory;
import org.apache.giraph.graph.partition.WorkerGraphPartitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MyLongRangePartitionerFactory<V extends Writable, E extends Writable, M extends Writable> 
extends RangePartitionerFactory<LongWritable, V, E, M>
implements Configurable
{

	private Configuration conf;
	@Override
	public MasterGraphPartitioner<LongWritable, V, E, M> createMasterGraphPartitioner() {
		return new MyLongRangePartitionerMaster<V, E, M>(getConf());
	}

	@Override
	public WorkerGraphPartitioner<LongWritable, V, E, M> createWorkerGraphPartitioner() {
		return new MyLongRangePatitionerWorker<V, E, M>(getConf());
	}
	
	
    public Configuration getConf() {
        return conf;
    }

   
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    public static int setRangePartitioner(GiraphJob job, String paritionFile) throws IOException
	{
		job.setGraphPartitionerFactoryClass(MyLongRangePartitionerFactory.class);
		PartitionString_N_Num ret=readPartitionString(job.getConfiguration(), paritionFile);
		job.getConfiguration().set(MyLongRangePartitionerMaster.RANGE_PARTITION_BOUNDARIES, ret.str);
		return ret.num;
	}
    
    public static class PartitionString_N_Num
    {
    	public int num;
    	public String str;
    }
    public static PartitionString_N_Num readPartitionString(Configuration conf, String paritionFile) throws IOException
    {
    	FileSystem fs=FileSystem.get(conf);
		BufferedReader in=new BufferedReader(new InputStreamReader(fs.open(new Path(paritionFile))));
		String line=null;
		
		PartitionString_N_Num ret=new PartitionString_N_Num();
		ret.str="";
		ret.num=0;
		while((line=in.readLine())!=null)
		{
			if(ret.num>0) 
			{
				long start=Long.parseLong(line.split("\t")[1]);
				ret.str+=(start-1)+",";
			}
			ret.num++;
		}
		in.close();
		ret.str=ret.str.substring(0, ret.str.length()-1);
		return ret;
    }
    
}
