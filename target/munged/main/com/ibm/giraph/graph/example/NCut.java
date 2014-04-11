package com.ibm.giraph.graph.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.graph.Aggregator;
import org.apache.giraph.graph.AggregatorWriter;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongLongNullLongVertex;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongLongNullNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongNullLongBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongNullXBinaryVertexInputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.graph.example.wcc.WCCVertex.SimpleLongLongNullVertexBinaryOutputFormat;

public class NCut extends AbstractSubGraph<LongWritable, LongWritable, NullWritable, LongWritable> 
implements Tool 
{
	LongWritable longbuff=new LongWritable();
	long numEdges=0;
	long numCrossEdges=0;
	@Override
	public void compute(
			final Partition<LongWritable, LongWritable, NullWritable, LongWritable> subgraph)
			throws IOException {
		numEdges=0;
		numCrossEdges=0;
		Iterator<BasicVertex<LongWritable, LongWritable, NullWritable, LongWritable>> 
		iterator=subgraph.getVertices().iterator();
		while (iterator.hasNext()) {
			LongLongNullLongVertex vertex = (LongLongNullLongVertex) iterator.next();
			numEdges+=vertex.getNumOutEdges();
			long[] neighbors=vertex.getNeighborsSimpleType();
			for(long dest: neighbors)
			{
				longbuff.set(dest);
				if(subgraph.getVertex(longbuff)==null)
					numCrossEdges++;
			}
			vertex.voteToHalt();
		}
		DoubleSumAggregator sumAggreg = (DoubleSumAggregator) getAggregator("ncut");
		double ncut=(double)numCrossEdges/(double)numEdges;
		sumAggreg.aggregate(ncut);
		LongMaxAggregator maxAggreg= (LongMaxAggregator) getAggregator("balance");
		maxAggreg.aggregate(subgraph.getEdgeCount());
		if(numEdges!=subgraph.getEdgeCount())
			throw new RuntimeException("numbers don't match: "+numEdges+" vs "+subgraph.getEdgeCount());
	}

	public static class SimpleNCutWorkerContext extends
	WorkerContext {

		@Override
		public void preApplication() 
		throws InstantiationException, IllegalAccessException {
			
			registerAggregator("ncut", DoubleSumAggregator.class);
			registerAggregator("balance", LongMaxAggregator.class);
			LongMaxAggregator maxAggreg= (LongMaxAggregator) getAggregator("balance");
			maxAggreg.setAggregatedValue(0);
			DoubleSumAggregator sumAggreg = 
					(DoubleSumAggregator) getAggregator("ncut");
			sumAggreg.setAggregatedValue(0);
		}

		@Override
		public void postApplication() {
		
		}

		@Override
		public void preSuperstep() {
			
		    useAggregator("ncut");
		    useAggregator("balance");
		//    useAggregator("parts");
		}

		@Override
		public void postSuperstep() {
		}
	}
	
	public static class NCutAggregateWriter implements AggregatorWriter 
	{
		public static String filename="temp.aggregate";
		private FSDataOutputStream output;
		
		@Override
	    public void initialize(Context context, long applicationAttempt)
	            throws IOException {
	        Path p = new Path(filename);
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	        output = fs.create(p, true);
	    }
	    
	    @Override
	    public void writeAggregator(Map<String, Aggregator<Writable>> map,
	            long superstep) throws IOException {

	      /*  for (Entry<String, Aggregator<Writable>> aggregator: map.entrySet()) {
	            aggregator.getValue().getAggregatedValue().write(output);
	        }*/
	    	if(map.containsKey("ncut"))
	    		map.get("ncut").getAggregatedValue().write(output);
	    	if(map.containsKey("balance"))
	    		map.get("balance").getAggregatedValue().write(output);
	        output.flush();
	    }

	    @Override
	    public void close() throws IOException {
	        output.close();
	    }
	}
	
	static class DummyVertex extends LongLongNullLongVertex
	{

		@Override
		public void compute(Iterator<LongWritable> msgIterator)
				throws IOException {
			
		}
		
	}
	
	@Override
	public int run(String[] argArray) throws Exception {
		
		if (argArray.length != 5) {
			System.err.println(
					"run: Must have 5 arguments <input path> <# of workers> <hash partition: true, range partition: false> "
					+ "<if hash partition, #partitions, otherwise range partiton file> <dummy output>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "NCut");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(DummyVertex.class);
		job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
				getClass(), SubGraph.class);
		job.setWorkerContextClass(SimpleNCutWorkerContext.class);
		job.setAggregatorWriterClass(NCutAggregateWriter.class);
		
		job.setVertexInputFormatClass(SimpleLongLongNullLongBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongLongNullVertexBinaryOutputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongLongNullXBinaryVertexInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongLongNullNeighborhood.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(argArray[0]));
		Path outpath=new Path(argArray[4]);
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		int nparts=0;
		if(Boolean.parseBoolean(argArray[2]))
		{
			job.getConfiguration().setInt(HashMasterPartitioner.USER_PARTITION_COUNT, Integer.parseInt(argArray[3]));
			nparts=Integer.parseInt(argArray[3]);
		}else
		{
			nparts=MyLongRangePartitionerFactory.setRangePartitioner(job, argArray[3]);
		}

		job.setWorkerConfiguration(Integer.parseInt(argArray[1]),
				Integer.parseInt(argArray[1]), 100.0f);
		if (job.run(true) == true) {
			
			CounterGroup group = job.getInternalJob().getCounters().getGroup("Giraph Stats");
			long numEdges=group.findCounter("Aggregate edges").getValue();
	        
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream input = 
                fs.open(new Path(NCutAggregateWriter.filename));
            int i;
            double sumncut=0;
            long maxSize=0;
            for (i = 0; ; i++) {
                try {
                    DoubleWritable ncut = new DoubleWritable();
                    ncut.readFields(input);
                    sumncut+=ncut.get();
                    LongWritable balance=new LongWritable();
                    balance.readFields(input);
                    maxSize=Math.max(maxSize, balance.get());
                } catch (IOException e) {
                    break;
                }
            }
            input.close();
           fs.delete(new Path(NCutAggregateWriter.filename), true);
           System.out.println("Hash Partition\tAvg NCut\tImBalance");
           System.out.println(argArray[2]+"\t"+(double)sumncut/(double)nparts+"\t"+(double)maxSize*(double)nparts/(double)numEdges);
			return 0;
		} else {
			return -1;
		}
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new NCut(), args);
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