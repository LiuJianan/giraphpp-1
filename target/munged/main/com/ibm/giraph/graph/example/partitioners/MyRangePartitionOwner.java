package com.ibm.giraph.graph.example.partitioners;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.BasicPartitionOwner;
import org.apache.hadoop.io.WritableComparable;

/**
 * Added the max key index in to the {@link PartitionOwner}.  Also can provide
 * a split hint if desired.
 *
 * @param <I> Vertex index type
 */
@SuppressWarnings("rawtypes")
public class MyRangePartitionOwner<I extends WritableComparable>
        extends BasicPartitionOwner {
    /** Max index for this partition */
    private I maxIndex;

    public MyRangePartitionOwner() {
    	super();
    }

    public MyRangePartitionOwner(I maxIndex) {
        super();
    	this.maxIndex = maxIndex;
    }
    public MyRangePartitionOwner(int partitionId, WorkerInfo workerInfo, I maxIndex) {
        super(partitionId, workerInfo);
        this.maxIndex = maxIndex;
    }

    public MyRangePartitionOwner(int partitionId,
                               WorkerInfo workerInfo,
                               WorkerInfo previousWorkerInfo,
                               String checkpointFilesPrefix, I maxIndex) {
        super(partitionId, workerInfo, previousWorkerInfo, checkpointFilesPrefix);
        this.maxIndex = maxIndex;
    }

    public I getMaxIndex() {
        return maxIndex;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        maxIndex = BspUtils.<I>createVertexIndex(getConf());
        maxIndex.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        maxIndex.write(output);
    }
}

