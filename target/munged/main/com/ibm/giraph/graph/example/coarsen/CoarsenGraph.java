package com.ibm.giraph.graph.example.coarsen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.list.LongArrayList;
import org.mortbay.log.Log;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;

import com.ibm.giraph.graph.example.coarsen.CoarsenMessageWritable;
import com.ibm.giraph.graph.example.coarsen.CoarsenVertex.SimpleGroupMembershipOutputFormat;
import com.ibm.giraph.graph.example.coarsen.CoarsenWorkerContext;
import com.ibm.giraph.graph.example.coarsen.CoarsenVertexValue;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongCoarsenVertexValueLongMNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SimpleLongCoarsenVertexValueLongMVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SkeletonNeighborhood;
import com.ibm.giraph.utils.VertexSetShuffler;

public class CoarsenGraph extends AbstractSubGraph<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> 
implements Tool{

	Random rand=new Random(System.currentTimeMillis());
	LongWritable tempLongWritable=new LongWritable();
	VertexSetShuffler<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> shuffler
	=new VertexSetShuffler<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable>();
	long numNodes=0;
	public static int LOCAL_MATCH_STEPS=9;
	public static final int NUM_ITERATIONS=20;
	
	void increaseCounter(long n)
	{
		LongSumAggregator sumAggreg = (LongSumAggregator) getAggregator(CoarsenWorkerContext.NUM_ACTIVE_NODES);
		sumAggreg.aggregate(n);
	}
	
	public static String edgeString(Iterator<LongWritable> edges)
	{
		String ret="";
		while(edges.hasNext())
			ret+=edges.next()+", ";
		return ret;
	}
	
	public static void processMergeMessage(CoarsenMessageWritable msg, DummyVertex vertex, 
			LongWritable tempLongWritable)
	{
		//remove merged node
		tempLongWritable.set(msg.msg[0]);
		vertex.removeEdge(tempLongWritable);
		//increase the size of the group
		vertex.getVertexValue().value+=msg.msg[1];
		//merge edge weight
		for(int i=2; i<msg.msg.length; i+=2)
		{
			tempLongWritable.set(msg.msg[i]);
			LongWritable w=vertex.getEdgeValue(tempLongWritable);
			if(w==null)
			{
				w=new LongWritable(msg.msg[i+1]);
				vertex.addEdge(new LongWritable(msg.msg[i]), w);
			}else
			{
				w.set(w.get()+msg.msg[i+1]);
			}
		}
	}
	
	private void processLocalMerge(DummyVertex vertex, DummyVertex target)
	{
		//directly merge
		target.removeEdge(vertex.getVertexId());
		target.getVertexValue().value+=vertex.oldweight;
		if(vertex.oldweight<0)
			throw new RuntimeException("vertex: "+vertex+" with oldweight "+vertex.oldweight);
		vertex.removeEdge(target.getVertexId());
		
		Iterator<LongWritable> p=vertex.iterator();
		
		
		while(p.hasNext())
		{
			LongWritable neighbor=p.next();
			//localmerge
			LongWritable w=target.getEdgeValue(neighbor);
			if(w==null)
				target.addEdge(neighbor, vertex.getEdgeValue(neighbor));
			else
				w.set(w.get()+vertex.getEdgeValue(neighbor).get());
		}
		
		//remove all edges
		vertex.removeAllEdges();
		
		vertex.voteToHalt();
	}
	
	private static void processMatch(Partition<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> subgraph, 
			DummyVertex vertex, LongWritable target)
	{
		CoarsenMessageWritable replacemsg=new CoarsenMessageWritable((byte)2, new long[]{vertex.getVertexId().get(), target.get()});
		
		Iterator<LongWritable> p=vertex.iterator();
		
		while(p.hasNext())
		{
			LongWritable neighbor=p.next();
			if(neighbor.get()!=target.get())
				processLocalReplacement(subgraph, neighbor, vertex, target, replacemsg);
		}
		vertex.oldweight=vertex.getVertexValue().value;
		vertex.getVertexValue().set((byte)2, target.get());
	}
	
	
	static void processRealReplacements(DummyVertex vertex, LongWritable tempLongWritable)
	{
		for(int i=0; i<vertex.blacklist.size(); i++)
		{
			long replaced=vertex.blacklist.get(i);
			long replacement=vertex.replacelist.get(i);
			tempLongWritable.set(replaced);
			LongWritable oldw=vertex.removeEdge(tempLongWritable);
			if(oldw==null)
				throw new RuntimeException("cannot replace "+tempLongWritable);
			if(replacement==vertex.getVertexId().get())
				continue;
			tempLongWritable.set(replacement);
			LongWritable w=vertex.getEdgeValue(tempLongWritable);
			if(w==null)
				vertex.addEdge(new LongWritable(replacement), oldw);
			else
				w.set(w.get()+oldw.get());
		}
		vertex.blacklist.clear();
		vertex.replacelist.clear();
	}
	
	private static void processLocalReplacement(Partition<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> subgraph,
			LongWritable neighbor, DummyVertex replaced, LongWritable replacement, CoarsenMessageWritable replacemsg) {
		
		if(subgraph==null)
		{
			replaced.sendMsg(neighbor, replacemsg);
		}else
		{
			DummyVertex neighborVertex =(DummyVertex) subgraph.getVertex(neighbor);
			if(neighborVertex==null)
			{
				replaced.sendMsg(neighbor, replacemsg);
			}
			else//actually do the replacement
			{
				neighborVertex.blacklist.add(replaced.getVertexId().get());
				neighborVertex.replacelist.add(replacement.get());
			}
		}	
	}

	public static void processReplacementMessage(CoarsenMessageWritable msg,
			DummyVertex vertex, LongWritable tempLongWritable)
	{
		vertex.blacklist.add(msg.msg[0]);
		vertex.replacelist.add(msg.msg[1]);
	}
	
	public static LongWritable chooseHeaviestEdge(DummyVertex vertex, Random rand)
	{
		//first find out the heaviest edge, break ties randomly
		long max=0; LongWritable argmax=null;
		Iterator<LongWritable> p=vertex.iterator();
		int n=0;
		while(p.hasNext())
		{
			LongWritable v=p.next();
			if(vertex.blacklist==null)
				throw new RuntimeException("vertex: "+vertex+" black list is null!");
			if(vertex.blacklist.indexOf(v.get())>=0)
				continue;
			long edgew=vertex.getEdgeValue(v).get();
			if(edgew>max)
			{
				max=edgew;
				argmax=v;
				n=1;
			}else if(edgew==max)
			{
				n++;
				if(rand.nextInt(n)==0)
					argmax=v;
			}
		}
		return argmax;
	}
	
	public static void sendMergeMsgs(DummyVertex vertex, long target, LongWritable tempLongWritable)
	{
		long[] neighborhood=new long[2*vertex.getNumOutEdges()];
		neighborhood[0]=vertex.getVertexId().get();
		neighborhood[1]=vertex.oldweight;
		if(vertex.oldweight<0)
			throw new RuntimeException("vertex: "+vertex+" with oldweight "+vertex.oldweight);
		Iterator<LongWritable> p=vertex.iterator();
		int i=2;
		LongWritable targetNode=null;
		while(p.hasNext())
		{
			LongWritable v=p.next();
			if(v.get()!=target)
			{
				neighborhood[i]=v.get();
				neighborhood[i+1]=vertex.getEdgeValue(v).get();
				i+=2;
			}else
				targetNode=v;
		}
		
		vertex.sendMsg(targetNode, new CoarsenMessageWritable((byte)1, neighborhood));
		vertex.removeAllEdges();
		
		vertex.voteToHalt();
	}
	
	public static void processAllMessages(Partition<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> subgraph, 
			DummyVertex vertex, boolean matchrequest, boolean mergerequest, boolean replace, boolean matchack, LongWritable tempLongWritable, Random rand)
	{
		
		Iterator<CoarsenMessageWritable> msgIterator = vertex.getMessages().iterator();
		if(!msgIterator.hasNext())
			return;
		
		if(mergerequest)//need to process merge request first
		{
			//first apply merge message if there is any
			while(msgIterator.hasNext())
			{
				CoarsenMessageWritable msg=msgIterator.next();
				if(msg.type==1)//can be multiple merge messages
				{
					processMergeMessage(msg, vertex, tempLongWritable);
				}
			}
		}
		
		//replace messages
		if(replace)
		{
			msgIterator = vertex.getMessages().iterator();
			while(msgIterator.hasNext())
			{
				CoarsenMessageWritable msg=msgIterator.next();
				//just process replacement messages
				if(msg.type==2)
					processReplacementMessage(msg, vertex, tempLongWritable);
			}	
		}
		
		
		if(matchrequest)
		{
			double max=0;
			int n=0;
			long argmax=-1;
			
			msgIterator = vertex.getMessages().iterator();
			while(msgIterator.hasNext())
			{
				CoarsenMessageWritable msg=msgIterator.next();
				if(msg.type==0)
				{
					if(vertex.getVertexValue().state==2 || vertex.getVertexValue().state==4)
						continue;
					else if(vertex.getVertexValue().state==3)//ignore request, if self has sent request
					{
						continue;
					}
					else
					{
						tempLongWritable.set(msg.msg[0]);
						double w=vertex.getEdgeValue(tempLongWritable).get();
						if(w>max)
						{
							max=w;
							argmax=msg.msg[0];
							n=1;
						}else if(w==max)//random sample among the max values
						{
							n++;
							if(rand.nextInt(n)==0)
								argmax=msg.msg[0];
						}
					}
				}
			}
			
			if(argmax>=0 && argmax!=vertex.getVertexId().get())
			{
				//send match acknowledgement
				LongWritable target=new LongWritable(argmax);
				vertex.sendMsg(target, new CoarsenMessageWritable((byte)3, new long[]{vertex.getVertexId().get()}));

				//send replacement messages
				processMatch(subgraph, vertex, target);
			}
		}
		
		if(matchack)
		{
			msgIterator = vertex.getMessages().iterator();
			while(msgIterator.hasNext())
			{
				CoarsenMessageWritable msg=msgIterator.next();
				if(msg.type==3)
				{
					vertex.getVertexValue().state=4;
					break;
				}
			}
		}
	}
	
	@Override
	public void compute(
			Partition<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> subgraph)
			throws IOException {
		
		Iterator<BasicVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable>> 
		iterator=subgraph.getVertices().iterator();
		
		//superstep 0 and superstep 1 merge the orphan nodes to their hubs
		//superstep 0 merges smaller vertices to larger vertices
		//superstep 1 merges larger vertices to smaller vertices
		
		numNodes=0;
		
		//remove orphan nodes first (smaller vertices merged to larger vertices)
		if(this.getSuperstep()==0)
		{
			while(iterator.hasNext())
			{
				MutableVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> vertex
				=(MutableVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable>) iterator.next();
				
				numNodes++;
				
				if(vertex.getNumOutEdges()==1)
				{
					LongWritable owner=vertex.iterator().next();
					MutableVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> target
					=(MutableVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable>) 
					subgraph.getVertex(owner);
					
					if(target!=null)
					{
						//directly merge
						target.removeEdge(vertex.getVertexId());
						target.getVertexValue().value+=vertex.getVertexValue().value;
						
						vertex.getVertexValue().set((byte)2, owner.get());
						vertex.voteToHalt();
						vertex.removeEdge(owner);
						numNodes--;
						
					}else
					{
						//send merge message
						if(vertex.getVertexId().get()<owner.get())
						{
							sendMsg(owner, new CoarsenMessageWritable((byte)1, new long[]{vertex.getVertexId().get(), vertex.getVertexValue().value}));
							vertex.getVertexValue().set((byte)2, owner.get());
							vertex.voteToHalt();
							vertex.removeEdge(owner);
							numNodes--;
						}
					}
				}else if(vertex.getNumOutEdges()==0)
				{
					vertex.voteToHalt();
					numNodes--;
				}
			}
			increaseCounter(numNodes);
			return;
		}
		
		if(this.getSuperstep()==1)//(larger vertices merged to the smaller vertices)
		{
			while(iterator.hasNext())
			{
				DummyVertex vertex =(DummyVertex) iterator.next();
								
				if(vertex.isHalted() && (vertex.getMessages()==null || vertex.getMessages().iterator()==null || !vertex.getMessages().iterator().hasNext()))
					continue;
				
				numNodes++;
				
				processAllMessages(subgraph, vertex, false, true, false, false, tempLongWritable, rand);
				
				if(vertex.getNumOutEdges()==1)
				{
					//Log.info("1-degree node: "+vertex);
					
					LongWritable owner=vertex.iterator().next();
					MutableVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> target
					=(MutableVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable>) 
					subgraph.getVertex(owner);
					if(target!=null)//this should not be called since this case has been caught in superstep 0
					{
						//directly merge
						target.removeEdge(vertex.getVertexId());
						target.getVertexValue().value+=vertex.getVertexValue().value;
						
						vertex.getVertexValue().set((byte)2, owner.get());
						vertex.voteToHalt();
						vertex.removeEdge(owner);
						numNodes--;
					//	Log.info("merged locally : "+vertex);
					}else
					{
						//send merge message
						if(vertex.getVertexId().get()>owner.get())
						{
							sendMsg(owner, new CoarsenMessageWritable((byte)1, new long[]{vertex.getVertexId().get(), vertex.getVertexValue().value}));
							vertex.getVertexValue().set((byte)2, owner.get());
							vertex.voteToHalt();
							vertex.removeEdge(owner);
							numNodes--;
						//	Log.info("merged to remote: "+vertex);
						}else
						{
							vertex.blacklist=new LongArrayList(vertex.getNumOutEdges()/2); //initialize the space
							vertex.replacelist=new LongArrayList(vertex.getNumOutEdges()/2); //initialize the space
						//	Log.info("initialize space: "+vertex.getVertexId());
						}
					}
				}else
				{
					vertex.blacklist=new LongArrayList(vertex.getNumOutEdges()/2); //initialize the space
					vertex.replacelist=new LongArrayList(vertex.getNumOutEdges()/2); //initialize the space
				//	Log.info("initialize space: "+vertex.getVertexId());
				}
			}
			increaseCounter(numNodes);
			return;
		}
		
		
		long step=(this.getSuperstep()-2)%LOCAL_MATCH_STEPS;
		boolean smallToBig=(step%4==0);
		
		for(BasicVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable> v: shuffler.shuffle(subgraph.getVertices(), rand))
		{
			if(v.isHalted() && (v.getMessages()==null || v.getMessages().iterator()==null || !v.getMessages().iterator().hasNext()))
				continue;
			
			if(!v.isHalted())
				numNodes++;
			
			DummyVertex vertex=(DummyVertex) v;
			
			if(step==0)
			{
				processAllMessages(subgraph, vertex, false, true, false, false, tempLongWritable, rand);
				//special checking for end of program
				if(getSuperstep()>NUM_ITERATIONS)
				{
					vertex.voteToHalt();
					continue;
				}
				
			}else if(step%2==0)
				processAllMessages(subgraph, vertex, false, false, true, true, tempLongWritable, rand);
			else
				processAllMessages(subgraph, vertex, true, false, true, false, tempLongWritable, rand);
						
			if(step==LOCAL_MATCH_STEPS-1)
			{	
				processRealReplacements(vertex, tempLongWritable);
				//send out merge messages
				if(vertex.getVertexValue().state==2)
				{
					numNodes--;
					tempLongWritable.set(vertex.getVertexValue().value);
					DummyVertex target=(DummyVertex) subgraph.getVertex(tempLongWritable);
					if(target!=null)//local merge
					{
						processLocalMerge(vertex, target);
					}else
						sendMergeMsgs(vertex, vertex.getVertexValue().value, tempLongWritable);//will send out merge messages
				}else if(vertex.getVertexValue().state==4)
					vertex.getVertexValue().state=1;
			}else if(step%2==0) //send match request
			{			
				//if already been matched, no need to send match request anymore
				if(vertex.getVertexValue().state==2 || vertex.getVertexValue().state==4)
					continue;
				
				//add randomness into the process
				if(rand.nextDouble()>=getMatchRequstProb(vertex.getNumOutEdges()))
					continue;
				//start the matching process
				LongWritable argmax=chooseHeaviestEdge(vertex, rand);
				
				if(argmax==null)
					continue;
				
				DummyVertex target=(DummyVertex) subgraph.getVertex(argmax);
				
				if(target!=null)//grant match immediately
				{
					if(target.getVertexValue().state==1 || target.getVertexValue().state==4)
					{
						processMatch(subgraph, vertex, argmax);//will send out replace messages
						target.getVertexValue().state=4;
					}
					
				}else if(  (smallToBig && vertex.getVertexId().get()<argmax.get()) 
				   ||(!smallToBig && vertex.getVertexId().get()>argmax.get()) )
				{	
					sendMsg(argmax, new CoarsenMessageWritable((byte)0, vertex.getVertexId().get()));//send match request messages
					//-2: match request has been sent
					vertex.getVertexValue().state=3;
				}
				
			}else //send out match ack
			{
				//will send out ack messages and replacement messages
				if(vertex.getVertexValue().state==3)
					vertex.getVertexValue().state=1;
			}
		}
		increaseCounter(numNodes);
	}

	public static double getMatchRequstProb(int numOutEdges) {
		if(numOutEdges==0)
			return 0;
		else if(numOutEdges==1)
			return 1.0;
		else
			return 1.0/Math.log1p(numOutEdges);
	}

	public static class DummyVertex extends EdgeListVertex<LongWritable, CoarsenVertexValue, LongWritable, CoarsenMessageWritable>
	{
		public LongArrayList blacklist=null;
		public LongArrayList replacelist=null;
		public long oldweight=-1;

		@Override
		public void compute(Iterator<CoarsenMessageWritable> msgIterator)
				throws IOException {
			
		}
		public String toString()
		{
			return super.toString()+"\n"+blacklist+"\n"+replacelist;
		}
		
		@Override
	    public void write(final DataOutput out) throws IOException {
			super.write(out);
			out.writeLong(oldweight);
			if(blacklist==null)
				out.writeInt(0);
			else
			{
				out.writeInt(blacklist.size());
				long[] temp=blacklist.elements();
				for(int i=0; i<blacklist.size(); i++)
					out.writeLong(temp[i]);
			}
			
			if(replacelist==null)
				out.writeInt(0);
			else
			{
				out.writeInt(replacelist.size());
				long[] temp=replacelist.elements();
				for(int i=0; i<replacelist.size(); i++)
					out.writeLong(temp[i]);
			}
			
		}
		
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			oldweight=in.readLong();
			int size=in.readInt();
			if(size>0)
			{
				if(blacklist==null)
					blacklist=new LongArrayList(size);
				else
					blacklist.clear();
				for(int i=0; i<size; i++)
					blacklist.add(in.readLong());
			}else if(blacklist!=null)
				blacklist.clear();
			
			size=in.readInt();
			if(size>0)
			{
				if(replacelist==null)
					replacelist=new LongArrayList(size);
				else
					replacelist.clear();
				for(int i=0; i<size; i++)
					replacelist.add(in.readLong());
			}else if(replacelist!=null)
				replacelist.clear();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println(
					"run: Must have 4 arguments <input path> <# of workers> <output> <#partition>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "CoarsenGraph");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 10);
		job.setVertexClass(DummyVertex.class);
		job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
				getClass(), SubGraph.class);
		
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
		System.exit(ToolRunner.run(new CoarsenGraph(), args));
	}

	@Override
	public void readAuxiliaryDataStructure(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeAuxiliaryDataStructure(DataOutput output)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
