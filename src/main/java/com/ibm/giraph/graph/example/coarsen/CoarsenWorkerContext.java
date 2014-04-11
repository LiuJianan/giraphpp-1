package com.ibm.giraph.graph.example.coarsen;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CoarsenWorkerContext extends
WorkerContext {

	public static final String RECORD_FILE_NAME="record.file.name";
	public static final String NUM_ACTIVE_NODES="activeNodes";
	private static final Logger LOG = Logger.getLogger(CoarsenWorkerContext.class);
	boolean need_to_record=false;
	long[] activeNodes=null;
	@Override
	public void preApplication() 
	throws InstantiationException, IllegalAccessException {
		registerAggregator(NUM_ACTIVE_NODES, LongSumAggregator.class);
		if(getWorkerId()==1)
		{
			need_to_record=true;
			activeNodes=new long[CoarsenGraph.NUM_ITERATIONS+CoarsenGraph.LOCAL_MATCH_STEPS+1];
		}
	}

	@Override
	public void postApplication() {
		LongSumAggregator sumAggreg = 
			(LongSumAggregator) getAggregator(NUM_ACTIVE_NODES);
		if(need_to_record)
		{
			String fname=getContext().getConfiguration().get(RECORD_FILE_NAME);
			try {
				BufferedWriter out= new BufferedWriter(new OutputStreamWriter(FileSystem.get(getContext().getConfiguration()).create(new Path(fname))));
				for(int i=0; i<activeNodes.length; i++)
				{
					out.write(i+" "+activeNodes[i]+"\n");
					//LOG.info("~~~ write down: "+i+" "+activeNodes[i]+"\n");
				}
				out.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void preSuperstep() {	
	    LongSumAggregator sumAggreg = 
			(LongSumAggregator) getAggregator(NUM_ACTIVE_NODES);
	   // LOG.info("~~~ before superstep "+this.getSuperstep()+": "+sumAggreg.getAggregatedValueSimple());
	    if(need_to_record && this.getSuperstep()>=0)
	    	activeNodes[(int) this.getSuperstep()]=sumAggreg.getAggregatedValueSimple();
	    useAggregator(NUM_ACTIVE_NODES);
	    sumAggreg.setAggregatedValue(0L);
	}

	@Override
	public void postSuperstep() {
	}
	
	private int getWorkerId() {
		String nodePrefix = getContext().getConfiguration().get("mapred.task.id");
		int j = nodePrefix.lastIndexOf("_");
		nodePrefix = nodePrefix.substring(0, j);
		j = nodePrefix.lastIndexOf("_");
		nodePrefix = nodePrefix.substring(j + 1);
		int y = (new Integer(nodePrefix)).intValue();
		return y;
	}
}
