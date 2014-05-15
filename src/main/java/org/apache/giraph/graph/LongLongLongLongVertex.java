package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.utils.UnmodifiableLongArrayIterator;
import org.apache.hadoop.io.LongWritable;


import com.google.common.collect.Iterables;

/*
 * This class is added
 */

public abstract class LongLongLongLongVertex extends
        BasicVertex<LongWritable, LongWritable, LongWritable,LongWritable> {

    private long id;
    private long value;

    private long[] neighbors;
    private long[] edgeValues;
    
    private long[] messages;

    @Override
    public void initialize(LongWritable vertexId, LongWritable vertexValue,
            Map<LongWritable, LongWritable> edges,
            Iterable<LongWritable> messages) {
        id = vertexId.get();
        value = vertexValue.get();
        neighbors = new long[edges.size()];
        edgeValues = new long[edges.size()];
        
        int n = 0;
        for (Entry<LongWritable, LongWritable> neighbor : edges.entrySet()) {
            this.neighbors[n] = neighbor.getKey().get();
            this.edgeValues[n] = neighbor.getValue().get();
            n++;
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

    @Override
    public LongWritable getVertexId() {
        return new LongWritable(id);
    }
    
    public long getVertexIdSimpleType()
    {
    	return id;
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
    public LongWritable getEdgeValue(LongWritable targetVertexId) {
        return new LongWritable(0);
    }
    public long getSimpleEdgeValue(int idx) {
        return edgeValues[idx];
    }
    public LongWritable getEdgeID(int idx) {
        return new LongWritable(neighbors[idx]);
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
        
        for (int n = 0; n < edgeValues.length; n++) {
            out.writeLong(edgeValues[n]);
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
        edgeValues = new long[numEdges];
        for (int n = 0; n < numEdges; n++) {
        	edgeValues[n] = in.readLong();
        }
        
        
        int numMessages = in.readInt();
        messages = new long[numMessages];
        for (int n = 0; n < numMessages; n++) {
            messages[n] = in.readLong();
        }
    }
    
    public String toString()
    {
    	String str="ID: "+id+", value: "+value+", edges: [";
    	for(int i=0; i<neighbors.length; i++)
    		str+=neighbors[i]+"," + edgeValues[i] + ",";
    	return str+"]";
    }
}
