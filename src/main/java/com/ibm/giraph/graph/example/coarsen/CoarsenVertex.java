package com.ibm.giraph.graph.example.coarsen;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.list.LongArrayList;

import com.ibm.giraph.graph.example.coarsen.CoarsenMessageWritable;
import com.ibm.giraph.graph.example.coarsen.CoarsenWorkerContext;
import com.ibm.giraph.graph.example.coarsen.CoarsenVertexValue;
import com.ibm.giraph.graph.example.coarsen.CoarsenGraph.DummyVertex;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongCoarsenVertexValueLongMNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SimpleLongCoarsenVertexValueLongMVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongXXXBinaryVertexOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;

public class CoarsenVertex extends DummyVertex
implements Tool {
	Random rand=new Random(System.currentTimeMillis());
	LongWritable tempLongWritable=new LongWritable();
	
	void increaseCounter(long n)
	{
		LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator(CoarsenWorkerContext.NUM_ACTIVE_NODES);
		sumAggreg.aggregate(n);
	}
	
	@Override
	public void compute(Iterator<CoarsenMessageWritable> msgIterator)
			throws IOException {
		
		//merge orphan nodes to their hub (smaller vertices merged to larger vertices)
		if(this.getSuperstep()==0)
		{
			if(this.getNumOutEdges()==1)
			{
				//replace yourself
				Iterator<LongWritable> p=this.iterator();
				LongWritable owner=p.next();
				if(this.getVertexId().get()<owner.get())
				{
					CoarsenMessageWritable msg=new CoarsenMessageWritable((byte)1, 
							new long[]{this.getVertexId().get(), this.getVertexValue().value});
				//	LOG.info("send to "+owner.get()+" with message: "+msg);
					this.sendMsg(owner, msg);
					this.getVertexValue().set((byte)2, owner.get());
					this.voteToHalt();
					this.removeEdge(owner);
					return;
				}
			}else if(getNumOutEdges()==0)
			{
				voteToHalt();
				return;
			}
			
			increaseCounter(1);
			return;
			
		}
		
		if(this.getSuperstep()==1) //(larger vertices merged to the smaller vertices)
		{
			CoarsenGraph.processAllMessages(null, this, false, true, false, false, tempLongWritable, rand);
			if(this.getNumOutEdges()==1)
			{
				//replace yourself
				Iterator<LongWritable> p=this.iterator();
				LongWritable owner=p.next();
				if(this.getVertexId().get()>owner.get())
				{
					CoarsenMessageWritable msg=new CoarsenMessageWritable((byte)1, new long[]{this.getVertexId().get(), this.getVertexValue().value});
					this.sendMsg(owner, msg);
					this.getVertexValue().set((byte)2, owner.get());
					this.voteToHalt();
					this.removeEdge(owner);
					return;
				}
			}
			increaseCounter(1);
			blacklist=new LongArrayList(getNumOutEdges()/2); //initialize the space
			replacelist=new LongArrayList(getNumOutEdges()/2); //initialize the space
			return;
		}
		
		long step=(this.getSuperstep()-2)%CoarsenGraph.LOCAL_MATCH_STEPS;
		boolean smallToBig=(step%4==0);
	
		boolean increaseCount=true;
		if(step==0)
		{
			CoarsenGraph.processAllMessages(null, this, false, true, false, false, tempLongWritable, rand);
			//special checking for end of program
			if(getSuperstep()>CoarsenGraph.NUM_ITERATIONS)
			{
				this.voteToHalt();
				return;
			}
			
		}else if(step%2==0)
			CoarsenGraph.processAllMessages(null, this, false, false, true, true, tempLongWritable, rand);
		else
			CoarsenGraph.processAllMessages(null, this, true, false, true, false, tempLongWritable, rand);
				
		if(step==CoarsenGraph.LOCAL_MATCH_STEPS-1)
		{	
			CoarsenGraph.processRealReplacements(this, tempLongWritable);
			//send out merge messages
			if(this.getVertexValue().state==2)
			{
				tempLongWritable.set(this.getVertexValue().value);
				CoarsenGraph.sendMergeMsgs(this, this.getVertexValue().value, tempLongWritable);//will send out merge messages
				increaseCount=false;
			}else if(this.getVertexValue().state==4)
				this.getVertexValue().state=1;
		}else if(step%2==0) //send match request
		{			
			//if already been matched, no need to send match request anymore
			if(this.getVertexValue().state==2 || this.getVertexValue().state==4)
				return;
			
			//add randomness into the process
			if(rand.nextDouble()>=CoarsenGraph.getMatchRequstProb(this.getNumOutEdges()))
				return;
			//start the matching process
			LongWritable argmax=CoarsenGraph.chooseHeaviestEdge(this, rand);
			
			if(argmax==null)
				return;
			
			if(  (smallToBig && this.getVertexId().get()<argmax.get()) 
			   ||(!smallToBig && this.getVertexId().get()>argmax.get()) )
			{	
				sendMsg(argmax, new CoarsenMessageWritable((byte)0, this.getVertexId().get()));//send match request messages
				//-2: match request has been sent
				this.getVertexValue().state=3;
			}
			
		}else //send out match ack
		{
			//will send out ack messages and replacement messages
			if(this.getVertexValue().state==3)
				this.getVertexValue().state=1;
		}
		if(increaseCount)
			increaseCounter(1);
	}

	public static String msgString(Iterator<CoarsenMessageWritable> msgIterator)
	{
		String ret="";
		while(msgIterator.hasNext())
			ret+="\n"+msgIterator.next();
		return ret;
	}
	
	static class SimpleGroupMembershipOutputFormat 
	extends SimpleLongXXXBinaryVertexOutputFormat<CoarsenVertexValue, LongWritable, LongCoarsenVertexValueLongMNeighborhood>
	{
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println(
					"run: Must have 4 arguments <input path> <# of workers> <output> <#partition>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "CoarsenVertex");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 10);
		job.setVertexClass(CoarsenVertex.class);
		job.setWorkerContextClass(CoarsenWorkerContext.class);
		job.setVertexInputFormatClass(SimpleLongCoarsenVertexValueLongMVertexBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SkeletonNeighborhood.class);
		job.setVertexOutputFormatClass(SimpleGroupMembershipOutputFormat.class);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongCoarsenVertexValueLongMNeighborhood.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath=new Path(args[2]);
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		job.setWorkerConfiguration(Integer.parseInt(args[1]),
				Integer.parseInt(args[1]), 100.0f);
		job.getConfiguration().setInt(HashMasterPartitioner.USER_PARTITION_COUNT, Integer.parseInt(args[3]));
		job.getConfiguration().set(CoarsenWorkerContext.RECORD_FILE_NAME, outpath.toString()+".activenodes");
		job.getConfiguration().setInt(GiraphJob.ZOOKEEPER_SESSION_TIMEOUT, 600000);
		if (job.run(true) == true) {
			return 0;
		} else {
			return -1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CoarsenVertex(), args));
	}

}
