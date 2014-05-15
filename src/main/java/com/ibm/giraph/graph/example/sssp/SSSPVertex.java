package com.ibm.giraph.graph.example.sssp;


import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongDoubleDoubleDoubleVertex;
import org.apache.giraph.graph.LongDoubleDoubleNeighborhood;
import org.apache.giraph.graph.LongLongNullLongVertex;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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

public class SSSPVertex extends LongDoubleDoubleDoubleVertex
implements Tool {
	
	Configuration conf = null;
    /** Logger */
    private static final Logger LOG =
       Logger.getLogger(SSSPVertex.class);
    	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SSSPVertex(), args));
	}
	
	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	private boolean isSource() {
		
		return this.getVertexId().get() == 0;
	}
	@Override
	public void compute(Iterator<DoubleWritable> msgs) throws IOException {
		if (getSuperstep() == 0) {
		    setVertexValue(new DoubleWritable(Double.MAX_VALUE));
		}
		double minDist = isSource() ? 0d : Double.MAX_VALUE;
		while(msgs.hasNext()) {
		    minDist = Math.min(minDist, msgs.next().get());
		}

		if (minDist < getVertexValue().get()) {
			setVertexValue(new DoubleWritable(minDist));
		    for (int i = 0 ;i <  this.getNumEdges() ; i ++) {
		    	double distance = minDist + getSimpleEdgeValue(i);

		    	sendMessage(this.getEdgeID(i), new DoubleWritable(distance));
		    }
		}
		voteToHalt();
	}

	private void sendMessage(LongWritable edgeID, DoubleWritable doubleWritable) {
		// TODO Auto-generated method stub
		
	}

	public static class SimpleLongDoubleNullVertexBinaryOutputFormat 
	extends SimpleLongXXXBinaryVertexOutputFormat<DoubleWritable, DoubleWritable, LongDoubleDoubleNeighborhood>
	{
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			System.err.println(
					"run: Must have 6 arguments <input path> <output path> <# of workers> <hash partition: true, range partition: false>"
					+ "<if hash partition, #partitions, otherwise range partiton file> <hybrid model: true, otherwise: false>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "SSSPVertex");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(SSSPVertex.class);
		job.setVertexInputFormatClass(SimpleLongDoubleNullDoubleBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongDoubleNullVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongDoubleNullXVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongDoubleDoubleNeighborhood.class);
		
		job.setVertexCombinerClass(MinCombiner.class);
		
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
