package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.giraph.utils.UnmodifiableLongArrayIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.google.common.collect.Iterables;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;

/*
 * This class is added
 */

public abstract class LongLongNullLongVertex extends
        BasicVertex<LongWritable, LongWritable, NullWritable,LongWritable> {

    private long id;
    private long value;

    private long[] neighbors;
    private long[] messages;

    @Override
    public void initialize(LongWritable vertexId, LongWritable vertexValue,
            Map<LongWritable, NullWritable> edges,
            Iterable<LongWritable> messages) {
        id = vertexId.get();
        value = vertexValue.get();
        this.neighbors = new long[edges.size()];
        int n = 0;
        for (LongWritable neighbor : edges.keySet()) {
            this.neighbors[n++] = neighbor.get();
        }
        if(messages==null)
        {
        	this.messages=new long[0];
        	return;
        }
        this.messages = new long[Iterables.size(messages)];
        n = 0;
        for (LongWritable message : messages) {
            this.messages[n++] = message.get();
        }
    }
    
    public void initialize(long vertexId, long vertexValue,
    		SkeletonNeighborhood<LongWritable, NullWritable> edges,
            Iterable<LongWritable> messages) {
        id = vertexId;
        value = vertexValue;
        this.neighbors = new long[edges.getNumberEdges()];
        int n = 0;
        for(; n<neighbors.length; n++)
            this.neighbors[n] = edges.getEdgeID(n);
        
        if(messages==null)
        {
        	this.messages=new long[0];
        	return;
        }
        this.messages = new long[Iterables.size(messages)];
        n = 0;
        for (LongWritable message : messages) {
            this.messages[n++] = message.get();
        }
    }

    @Override
    public LongWritable getVertexId() {
        return new LongWritable(id);
    }
    
    public long getVertexIdSimpleType()
    {
    	return id;
    }
    
    public void setVertexIdSimpleType(long nid)
    {
    	id=nid;
    }

    @Override
    public LongWritable getVertexValue() {
        return new LongWritable(value);
    }
    
    public long getVertexValueSimpleType() {
        return value;
    }

    @Override
    public void setVertexValue(LongWritable vertexValue) {
        value = vertexValue.get();
    }
    
    public void setVertexValueSimpleType(long vertexValue) {
        value = vertexValue;
    }

    @Override
    public Iterator<LongWritable> iterator() {
        return new UnmodifiableLongArrayIterator(neighbors);
    }
    
    public long[] getNeighborsSimpleType()
    {
    	return neighbors;
    }

    @Override
    public NullWritable getEdgeValue(LongWritable targetVertexId) {
        return NullWritable.get();
    }

    @Override
    public boolean hasEdge(LongWritable targetVertexId) {
        for (long neighbor : neighbors) {
            if (neighbor == targetVertexId.get()) {
                return true;
            }
        }
        return false;
    }
    
    public boolean hasEdgeSimpleType(long targetVertexId) {
        for (long neighbor : neighbors) {
            if (neighbor == targetVertexId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getNumOutEdges() {
        return neighbors.length;
    }

    @Override
    public void sendMsgToAllEdges(final LongWritable message) {
        for (long neighbor : neighbors) {
            sendMsg(new LongWritable(neighbor), message);
        }
    }

    @Override
    public Iterable<LongWritable> getMessages() {
        return new Iterable<LongWritable>() {
            @Override
            public Iterator<LongWritable> iterator() {
                return new UnmodifiableLongArrayIterator(messages);
            }
        };
    }
    
    public long[] getMessagesSimpleType() {
       return messages;
    }

    public void putMessages(Iterable<LongWritable> newMessages) {
        messages = new long[Iterables.size(newMessages)];
        int n = 0;
        for (LongWritable message : newMessages) {
            messages[n++] = message.get();
        }
    }

    void releaseResources() {
        messages = new long[0];
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(value);
        out.writeInt(neighbors.length);
        for (int n = 0; n < neighbors.length; n++) {
            out.writeLong(neighbors[n]);
        }
        out.writeInt(messages.length);
        for (int n = 0; n < messages.length; n++) {
            out.writeLong(messages[n]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        value = in.readLong();
        int numEdges = in.readInt();
        neighbors = new long[numEdges];
        for (int n = 0; n < numEdges; n++) {
            neighbors[n] = in.readLong();
        }
        int numMessages = in.readInt();
        messages = new long[numMessages];
        for (int n = 0; n < numMessages; n++) {
            messages[n] = in.readLong();
        }
    }
}
