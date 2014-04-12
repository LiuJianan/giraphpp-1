package com.ibm.giraph.subgraph.example.coarsen;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.list.LongArrayList;

import com.ibm.giraph.subgraph.example.coarsen.ParMetisCoarsenSubGraph.DummyVertex;

public class PsedoCode {/*

	void processOneDegreeVertex(vertex, superstep)
	{
		if(vertex.getNumOutEdges()==1)
		{
			target=getVertex(vertex.iterator.next());
			if(isInternalVertex(target.getVertexID()))
				match immediately
				else if(superstep==0 && vertex.getVertexID()<target.VertexID() || superstep==1 && vertex.getVertexID().target.VertexID())
					sendMsg(target.getVertexID(), MergeRequest(vertex.getVertexID(), vertex.getVertexValue().weight))
		}
	}
	void compute()
	{
		while(iterator.hasNext())
		{
			if(getSuperstep()<=1)
			{
				processMergeMessages(vertex.getMessages())
				processOneDegreeVertex(vertex, superstep)
				return;
			}
		}
		
	
		long step=(this.getSuperstep()-2)%LOCAL_MATCH_STEPS;
		boolean smallToBig=(step%4==0);
		
		for(v: randomShuffle(subgraph.getVertices()))
		{	
			//Log.info("@@@@@@ after process all messages for vertex: "+vertex);
			
			if(step==LOCAL_MATCH_STEPS-1)
			{	
				processMatchNotification();
			//	Log.info("after real replacements: "+vertex);
				//send out merge messages
				if(vertex.getVertexValue().state==MergeToRemove)
				{
					sendMsg(remoteNode, MergeRequest(vertex.id, weight and edges with their weights))
				}else if (MergeTolocal)
					Merge Immediately
				else if(vertex.getVertexValue() merge receipient)
					vertex.getVertexValue().state=open;
			}else if(step%2==0) //send match request
			{			
				if(vertex mergetToremove or merge receipient)
					continue;
				
				if(rand.nextDouble()>=getMatchRequstProb(vertex.getNumOutEdges()))
					continue;
				
				//start the matching process
				LongWritable argmax=chooseHeaviestEdge(vertex, rand);
				//Log.info("should send match request to "+argmax);
				
				if(argmax==null)
					continue;
				
				DummyVertex target=(DummyVertex) subgraph.getVertex(argmax);
				
				if(target!=null)//grant match immediately
				{
					if(target.getVertexValue().state==1 || target.getVertexValue().state==4)
					{
						//Log.info("** process match locally: "+vertex+", to match with "+argmax);
						processMatch(subgraph, vertex, argmax);//will send out replace messages
						target.getVertexValue().state=4;
						//Log.info("** after match locally itself: "+vertex);
						//Log.info("** after match locally target: "+target);
					}
					
				}else if(  (smallToBig && vertex.getVertexId().get()<argmax.get()) 
				   ||(!smallToBig && vertex.getVertexId().get()>argmax.get()) )
				{	
					sendMsg(argmax, new ParMetisCoarsenMessageWritable((byte)0, vertex.getVertexId().get()));//send match request messages
					//-2: match request has been sent
					vertex.getVertexValue().state=3;
					//Log.info("** send match request: "+vertex+", to match with "+argmax);
				}
				
			}else //send out match ack
			{
				//will send out ack messages and replacement messages
				if(vertex.getVertexValue().state==3)
					vertex.getVertexValue().state=1;
			}
		}

	}*/
}
