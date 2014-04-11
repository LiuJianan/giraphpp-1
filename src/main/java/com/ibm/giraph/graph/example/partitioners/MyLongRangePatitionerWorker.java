package com.ibm.giraph.graph.example.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionExchange;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.RangeWorkerPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MyLongRangePatitionerWorker<V extends Writable, E extends Writable, M extends Writable> 
extends RangeWorkerPartitioner<LongWritable, V, E, M>{

	/** Mapping of the vertex ids to the {@link PartitionOwner} */
    protected NavigableMap<Long, MyLongRangePartitionOwner> vertexRangeMap =
        new TreeMap<Long, MyLongRangePartitionOwner>();

    @Override
    public PartitionOwner createPartitionOwner() {
        return new MyLongRangePartitionOwner();
    }

    @Override
    public Collection<? extends PartitionOwner> getPartitionOwners() {
        return vertexRangeMap.values();
    }
	public MyLongRangePatitionerWorker(Configuration conf) {
		String str=conf.get(MyLongRangePartitionerMaster.RANGE_PARTITION_BOUNDARIES);
		if(str==null)
			throw new RuntimeException("cannot read the partition boundaries!");
		String[] strs=str.split(",");
		for(int i=0; i<strs.length; i++)
		{
			long maxVertexID=Long.parseLong(strs[i]);
			vertexRangeMap.put(maxVertexID, new MyLongRangePartitionOwner(maxVertexID) );
		}
	}
	
	 @Override
	    public PartitionOwner getPartitionOwner(LongWritable vertexId) {
	        // Find the partition owner based on the maximum partition id.
	        // If the vertex id exceeds any of the maximum partition ids, give
	        // it to the last one
	        if (vertexId == null) {
	            throw new IllegalArgumentException(
	                "getPartitionOwner: Illegal null vertex id");
	        }
	        Long maxVertexIndex = vertexRangeMap.ceilingKey(vertexId.get());
	        if (maxVertexIndex == null) {
	            return vertexRangeMap.lastEntry().getValue();
	        } else {
	            return vertexRangeMap.get(maxVertexIndex);
	        }
	    }
	
	@Override
	public Collection<PartitionStats> finalizePartitionStats(
			Collection<PartitionStats> workerPartitionStats,
			Map<Integer, Partition<LongWritable, V, E, M>> partitionMap) {
		return workerPartitionStats;
	}

	@Override
	public PartitionExchange updatePartitionOwners(WorkerInfo myWorkerInfo,
			Collection<? extends PartitionOwner> masterSetPartitionOwners,
			Map<Integer, Partition<LongWritable, V, E, M>> partitionMap) {
		
		vertexRangeMap.clear();
		for(PartitionOwner owner: masterSetPartitionOwners)
		{
			vertexRangeMap.put(((MyLongRangePartitionOwner)owner).getMaxIndex(), 
					(MyLongRangePartitionOwner) owner);
		}

        Set<WorkerInfo> dependentWorkerSet = new HashSet<WorkerInfo>();
        Map<WorkerInfo, List<Integer>> workerPartitionOwnerMap =
            new HashMap<WorkerInfo, List<Integer>>();
        for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
            if (partitionOwner.getPreviousWorkerInfo() == null) {
                continue;
            } else if (partitionOwner.getWorkerInfo().equals(
                       myWorkerInfo) &&
                       partitionOwner.getPreviousWorkerInfo().equals(
                       myWorkerInfo)) {
                throw new IllegalStateException(
                    "updatePartitionOwners: Impossible to have the same " +
                    "previous and current worker info " + partitionOwner +
                    " as me " + myWorkerInfo);
            } else if (partitionOwner.getWorkerInfo().equals(myWorkerInfo)) {
                dependentWorkerSet.add(partitionOwner.getPreviousWorkerInfo());
            } else if (partitionOwner.getPreviousWorkerInfo().equals(
                    myWorkerInfo)) {
                if (workerPartitionOwnerMap.containsKey(
                        partitionOwner.getWorkerInfo())) {
                    workerPartitionOwnerMap.get(
                        partitionOwner.getWorkerInfo()).add(
                            partitionOwner.getPartitionId());
                } else {
                    List<Integer> partitionOwnerList = new ArrayList<Integer>();
                    partitionOwnerList.add(partitionOwner.getPartitionId());
                    workerPartitionOwnerMap.put(partitionOwner.getWorkerInfo(),
                                                partitionOwnerList);
                }
            }
        }

        return new PartitionExchange(dependentWorkerSet,
                                     workerPartitionOwnerMap);
	}

}
