package com.ibm.giraph.graph.example.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongDoubleNullDoubleVertex;
import org.apache.giraph.graph.LongDoubleNullNeighborhood;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongDoubleNullDoubleBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongDoubleNullXVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongXXXBinaryVertexOutputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.utils.MapRedudeUtils;

public class DeltaPRVertex extends LongDoubleNullDoubleVertex
implements Tool {
	Configuration conf = null;
    /** Logger */
    private static final Logger LOG =
       Logger.getLogger(DeltaPRVertex.class);
    	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new DeltaPRVertex(), args));
	}
	
	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	@Override
	public void compute(Iterator<DoubleWritable> msgs) throws IOException {
		if(getSuperstep()>= DeltaPRGraph.MAX_SUPERSTEPS)
		{
			this.voteToHalt();
			return;
		}
			
		double delta=0;
		if(getSuperstep()==0)
		{
			this.setVertexValueSimpleType(0);
			delta=0.15;
		}
		
		while(msgs.hasNext())
		{
			double msg=msgs.next().get();
			delta+=msg;
		}
		
		this.voteToHalt();
		if(delta==0) return;//if no change, do nothing
		
		double newValue=this.getVertexValueSimpleType()+delta;
		this.setVertexValueSimpleType(newValue);
		this.sendMsgToAllEdges(new DoubleWritable(0.85*delta/(double)this.getNumOutEdges()));		
	}

	public static class SimpleLongDoubleNullVertexBinaryOutputFormat 
	extends SimpleLongXXXBinaryVertexOutputFormat<DoubleWritable, NullWritable, LongDoubleNullNeighborhood>
	{
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			System.err.println(
					"run: Must have 6 arguments <input path> <output path> <# of workers> <hash partition: true, range partition: false> "
					+ "<if hash partition, #partitions, otherwise range partiton file> <hybrid model: true, otherwise: false>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "DeltaPRVertex");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(DeltaPRVertex.class);
		job.setVertexInputFormatClass(SimpleLongDoubleNullDoubleBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongDoubleNullVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongDoubleNullXVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongDoubleNullNeighborhood.class);
		
		job.setVertexCombinerClass(SumCombiner.class);
		
		if(Boolean.parseBoolean(args[3]))
			job.getConfiguration().setInt(HashMasterPartitioner.USER_PARTITION_COUNT, Integer.parseInt(args[4]));
		else
			MyLongRangePartitionerFactory.setRangePartitioner(job, args[4]);
		job.setWorkerConfiguration(Integer.parseInt(args[2]),
				Integer.parseInt(args[2]), 100.0f);
		if(Boolean.parseBoolean(args[5]))
			job.getConfiguration().setInt(GiraphJob.NUM_SUB_STEPS_PER_ITERATION, 1);
		else
			job.getConfiguration().setInt(GiraphJob.NUM_SUB_STEPS_PER_ITERATION, 0);
		if (job.run(true) == true) {
			return 0;
		} else {
			return -1;
		}
	}
}
