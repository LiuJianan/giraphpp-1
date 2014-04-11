package com.ibm.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.giraph.graph.Aggregator;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.giraph.graph.GraphState;

/**
 * 
 * This is an abstract implementation of the SubGraphInterface.
 * It provides the user facing compute method. 
 * 
 * 
 * @param <I>
 *            Vertex index value
 * @param <V>
 *            Vertex value
 * @param <E>
 *            Edge value
 * @param <M>
 *            Message value
 * 
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractSubGraph<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
		implements SubGraph<I, V, E, M> {
	protected Context context; 
	protected VertexCombiner<I, M> sendersideCombiner;
	/** Formats the Superstep Counter **/
	protected static final DecimalFormat counterformat = new DecimalFormat  ( ",000" );
	private GraphState<I, V, E, M> graphState;
	private Configuration conf = null;
	
	/**
	 * Must be implemented by the user in order to do the computation for a
	 * subgraph within one superstep. 
	 * 
	 * @param collection a collection of vertices to compute
	 * 
	 */
	public abstract void compute(Partition<I, V, E, M> subgraph) throws IOException;

	/**
	 * This is called by the GiraphPlusPlus framework
	 * 
	 * @param collection a collection of vertices to compute
	 * @param context the current GiraphMapper context
	 * @param sendersideCombiner the senderside Combiner to use.
	 */
	public final void compute(Partition<I, V, E, M> subgraph, Context context, 
			VertexCombiner<I, M> sendersideCombiner, GraphState<I, V, E, M> gstate) 
	throws IOException
	{
		this.context = context;
		this.sendersideCombiner = sendersideCombiner;
		this.graphState=gstate;
		compute(subgraph);
	}
	
	/**
	 * return the current superstep
	 */
	public long getSuperstep()
	{
		return graphState.getSuperstep();
	}
	
	/**
	 * return the current graph state
	 */
	GraphState<I, V, E, M> getGraphState() {
        return graphState;
    }

    /**
     * Set the graph state for all workers
     *
     * @param graphState Graph state for all workers
     */
    void setGraphState(GraphState<I, V, E, M> graphState) {
        this.graphState = graphState;
    }

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
     public Mapper.Context getContext() {
         return getGraphState().getContext();
     }

    /**
     * Get the worker context
     *
     * @return WorkerContext context
     */
    public WorkerContext getWorkerContext() {
        return getGraphState().getGraphMapper().getWorkerContext();
    }

    /**
     * register an aggregator
     * @param <A>
     * @param name
     * @param aggregatorClass
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final <A extends Writable> Aggregator<A> registerAggregator(
            String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        return getGraphState().getGraphMapper().getAggregatorUsage().
            registerAggregator(name, aggregatorClass);
    }

    /**
     * get an aggregator by name
     * @param name
     * @return
     */
    public final Aggregator<? extends Writable> getAggregator(String name) {
        return getGraphState().getGraphMapper().getAggregatorUsage().
            getAggregator(name);
    }

    /**
     * activate the use of an aggregator
     * @param name
     * @return
     */
    public final boolean useAggregator(String name) {
        return getGraphState().getGraphMapper().getAggregatorUsage().
            useAggregator(name);
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    public long getNumVertices() {
        return getGraphState().getNumVertices();
    }
    
    public long getNumEdges() {
        return getGraphState().getNumEdges();
    }
    
 /*   public final void sendMsg(I id, M msg) {
        if (msg == null) {
            throw new IllegalArgumentException(
                    "sendMsg: Cannot send null message to " + id);
        }
        
        if(getGraphState().getCurrentPartition().getVertex(id)!=null)
        	getGraphState().addInternalMessage(id, msg);
        else
        	getGraphState().addExternalMessage(id, msg);
       // Log.info("@@ send message: "+msg+" to vertex: "+id);
      //   getGraphState().getWorkerCommunications().sendMessageReq(id, msg);
    }
*/   
    /**
     * send a message msg to vertex id
     */
    public final void sendMsg(I id, M msg) {
        if (msg == null) {
            throw new IllegalArgumentException(
                    "sendMsg: Cannot send null message to " + id);
        }
        getGraphState().getWorkerCommunications().sendMessageReq(id, msg);
    }
    
    /**
     * write auxiliary data structures down for check pointing
     */
    public abstract void writeAuxiliaryDataStructure(DataOutput output) throws IOException; 
    
    /**
     * read auxiliary data structures for check pointing
     */
	public abstract void readAuxiliaryDataStructure(DataInput input) throws IOException;
}
