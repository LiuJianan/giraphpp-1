package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.giraph.utils.UnmodifiableDoubleArrayIterator;
import org.apache.giraph.utils.UnmodifiableLongArrayIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.google.common.collect.Iterables;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;

/*
 * This class is added
 */

public abstract class LongDoubleNullDoubleVertex extends
        BasicVertex<LongWritable, DoubleWritable, NullWritable,DoubleWritable> {

    private long id;
    private double value;

    private long[] neighbors;
    private double[] messages;

    @Override
    public void initialize(LongWritable vertexId, DoubleWritable vertexValue,
            Map<LongWritable, NullWritable> edges,
            Iterable<DoubleWritable> messages) {
        id = vertexId.get();
        value = vertexValue.get();
        this.neighbors = new long[edges.size()];
        int n = 0;
        for (LongWritable neighbor : edges.keySet()) {
            this.neighbors[n++] = neighbor.get();
        }
        if(messages==null)
        {
        	this.messages=new double[0];
        	return;
        }
        
        this.messages = new double[Iterables.size(messages)];
        n = 0;
        for (DoubleWritable message : messages) {
            this.messages[n++] = message.get();
        }
    }
    
    public void initialize(long vertexId, double vertexValue,
    		SkeletonNeighborhood<LongWritable, NullWritable> edges,
            Iterable<DoubleWritable> messages) {
        id = vertexId;
        value = vertexValue;
        this.neighbors = new long[edges.getNumberEdges()];
        int n = 0;
        for(; n<neighbors.length; n++)
            this.neighbors[n] = edges.getEdgeID(n);
        if(messages==null)
        {
        	this.messages=new double[0];
        	return;
        }
        
        this.messages = new double[Iterables.size(messages)];
        n = 0;
        for (DoubleWritable message : messages) {
            this.messages[n++] = message.get();
        }
    }
    
    public void initialize(long vertexId, 
    		LongDoubleNullNeighborhood neighborhood,
            Iterable<DoubleWritable> messages) {
        id = vertexId;
        value = neighborhood.getVertexValue();
        this.neighbors = new long[neighborhood.getNumberEdges()];
        int n = 0;
        for(; n<neighbors.length; n++)
            this.neighbors[n] = neighborhood.getEdgeID(n);
        if(messages==null)
        {
        	this.messages=new double[0];
        	return;
        }
        
        this.messages = new double[Iterables.size(messages)];
        n = 0;
        for (DoubleWritable message : messages) {
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
    public DoubleWritable getVertexValue() {
        return new DoubleWritable(value);
    }
    
    public double getVertexValueSimpleType() {
        return value;
    }

    @Override
    public void setVertexValue(DoubleWritable vertexValue) {
        value = vertexValue.get();
    }
    
    public void setVertexValueSimpleType(double vertexValue) {
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
    public void sendMsgToAllEdges(final DoubleWritable message) {
        for (long neighbor : neighbors) {
            sendMsg(new LongWritable(neighbor), message);
        }
    }

    @Override
    public Iterable<DoubleWritable> getMessages() {
        return new Iterable<DoubleWritable>() {
            @Override
            public Iterator<DoubleWritable> iterator() {
                return new UnmodifiableDoubleArrayIterator(messages);
            }
        };
    }
    
    public double[] getMessagesSimpleType() {
       return messages;
    }

    public void putMessages(Iterable<DoubleWritable> newMessages) {
        messages = new double[Iterables.size(newMessages)];
        int n = 0;
        for (DoubleWritable message : newMessages) {
            messages[n++] = message.get();
        }
    }

    void releaseResources() {
        messages = new double[0];
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeDouble(value);
        out.writeInt(neighbors.length);
        for (int n = 0; n < neighbors.length; n++) {
            out.writeLong(neighbors[n]);
        }
        out.writeInt(messages.length);
        for (int n = 0; n < messages.length; n++) {
            out.writeDouble(messages[n]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        value = in.readDouble();
        int numEdges = in.readInt();
        neighbors = new long[numEdges];
        for (int n = 0; n < numEdges; n++) {
            neighbors[n] = in.readLong();
        }
        int numMessages = in.readInt();
        messages = new double[numMessages];
        for (int n = 0; n < numMessages; n++) {
            messages[n] = in.readDouble();
        }
    }
    
    public String toString()
    {
    	String str="ID: "+id+", value: "+value+", edges: [";
    	for(int i=0; i<neighbors.length; i++)
    		str+=neighbors[i]+",";
    	return str+"]";
    }
}
