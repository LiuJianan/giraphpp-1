package com.ibm.giraph.graph.example.partitioners;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.BasicPartitionOwner;

public class MyLongRangePartitionOwner extends BasicPartitionOwner {
    /** Max index for this partition */
    private long maxIndex;

    public MyLongRangePartitionOwner() {
    	super();
    }

    public MyLongRangePartitionOwner(long maxIndex) {
        super();
    	this.maxIndex = maxIndex;
    }
    public MyLongRangePartitionOwner(int partitionId, WorkerInfo workerInfo, long maxIndex) {
        super(partitionId, workerInfo);
        this.maxIndex = maxIndex;
    }

    public MyLongRangePartitionOwner(int partitionId,
                               WorkerInfo workerInfo,
                               WorkerInfo previousWorkerInfo,
                               String checkpointFilesPrefix, long maxIndex) {
        super(partitionId, workerInfo, previousWorkerInfo, checkpointFilesPrefix);
        this.maxIndex = maxIndex;
    }

    public long getMaxIndex() {
        return maxIndex;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        maxIndex = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeLong(maxIndex);
    }

	
}
