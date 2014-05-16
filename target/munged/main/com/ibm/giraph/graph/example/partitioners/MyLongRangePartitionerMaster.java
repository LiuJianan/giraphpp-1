package com.ibm.giraph.graph.example.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.PartitionBalancer;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.RangeMasterPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MyLongRangePartitionerMaster<V extends Writable, E extends Writable, M extends Writable> 
extends RangeMasterPartitioner<LongWritable, V, E, M> {

	public static final String RANGE_PARTITION_BOUNDARIES="range.partition.boundaries";
	/** Provided configuration */
    private Configuration conf;
    /** Save the last generated partition owner list */
    private List<PartitionOwner> partitionOwnerList;
    long[] boundaries=null;
	public MyLongRangePartitionerMaster(Configuration conf) {
		this.conf=conf;
		String str=conf.get(RANGE_PARTITION_BOUNDARIES);
		if(str==null)
			throw new RuntimeException("cannot read the partition boundaries!");
		String[] strs=str.split(",");
		boundaries=new long[strs.length];
		for(int i=0; i<strs.length; i++)
			boundaries[i]=Long.parseLong(strs[i]);
	}

	@Override
	public Collection<PartitionOwner> createInitialPartitionOwners(
			Collection<WorkerInfo> availableWorkerInfos, int maxWorkers) {
	
		if (availableWorkerInfos.isEmpty()) {
	            throw new IllegalArgumentException(
	                "createInitialPartitionOwners: No available workers");
	        }
	        List<PartitionOwner> ownerList = new ArrayList<PartitionOwner>();
	        Iterator<WorkerInfo> workerIt = availableWorkerInfos.iterator();
	     
	        //round robin fashion
	        for (int i = 0; i < boundaries.length; ++i) {
	            MyLongRangePartitionOwner owner = new MyLongRangePartitionOwner(i, workerIt.next(), boundaries[i]);
	            if (!workerIt.hasNext()) {
	                workerIt = availableWorkerInfos.iterator();
	            }
	            ownerList.add(owner);
	        }
	        this.partitionOwnerList = ownerList;
	        return ownerList;
	}

	@Override
    public PartitionStats createPartitionStats() {
        return new PartitionStats();
    }
	
	@Override
	public Collection<PartitionOwner> generateChangedPartitionOwners(
			Collection<PartitionStats> allPartitionStatsList,
			Collection<WorkerInfo> availableWorkers, int maxWorkers,
			long superstep) {
		
	        return PartitionBalancer.balancePartitionsAcrossWorkers(
	            conf,
	            partitionOwnerList,
	            allPartitionStatsList,
	            availableWorkers);
	}

	@Override
	public Collection<PartitionOwner> getCurrentPartitionOwners() {
		return partitionOwnerList;
	}
	
}
