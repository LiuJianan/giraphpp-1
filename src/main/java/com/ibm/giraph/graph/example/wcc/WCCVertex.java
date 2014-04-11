package com.ibm.giraph.graph.example.wcc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongLongNullLongVertex;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.LongLongNullNeighborhood;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongNullLongBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongNullXBinaryVertexInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongXXXBinaryVertexOutputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.utils.MapRedudeUtils;

public class WCCVertex extends LongLongNullLongVertex
		implements Tool {
	private static final Logger LOG = Logger.getLogger(WCCVertex.class);
	Configuration conf = null;

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WCCVertex(), args));
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
	public void compute(Iterator<LongWritable> msgs) throws IOException {
		
		if (getSuperstep() == 0) 
			setVertexValueSimpleType(getVertexIdSimpleType());
		LongWritable min = getVertexValue();
		while (msgs.hasNext()) {
			LongWritable tmp = msgs.next();
			if (min.get() > tmp.get()) {
				min = tmp;
			}
		}
		
		if(getSuperstep()==0 || min.get() < getVertexValue().get()){
			setVertexValue(min);
			sendMsgToAllEdges(getVertexValue());
		}
		voteToHalt();
	}

	public static class SimpleLongLongNullVertexBinaryOutputFormat 
	extends SimpleLongXXXBinaryVertexOutputFormat<LongWritable, NullWritable, LongLongNullNeighborhood>
	{
		
	}
	public static class LongLongMinCombiner extends MinCombiner<LongWritable, LongWritable>
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
		GiraphJob job = new GiraphJob(getConf(), "WCCVertex");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(WCCVertex.class);
		job.setVertexInputFormatClass(SimpleLongLongNullLongBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongLongNullVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongLongNullXBinaryVertexInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongLongNullNeighborhood.class);
		job.setVertexCombinerClass(LongLongMinCombiner.class);
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