package com.ibm.giraph.graph.example.reachability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongDoubleNullDoubleVertex;
import org.apache.giraph.graph.LongDoubleNullNeighborhood;
import org.apache.giraph.graph.LongLongLongLongVertex;
import org.apache.giraph.graph.LongLongLongNeighborhood;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongLongVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.graph.example.reachability.ReachabilityVertex.SimpleLongLongLongVertexBinaryOutputFormat;
import com.ibm.giraph.utils.MapRedudeUtils;

public class ReachabilityGraph
		extends
		AbstractSubGraph<LongWritable, LongWritable, LongWritable, LongWritable>
		implements Tool {

	private static final Logger LOG = Logger.getLogger(ReachabilityGraph.class);

	@Override
	public void compute(
			final Partition<LongWritable, LongWritable, LongWritable, LongWritable> subgraph)
			throws IOException {
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.err
					.println("run: Must have 5 arguments <input path> <output path> <# of workers> <hash partition: true, range partition: false> "
							+ "<if hash partition, #partitions, otherwise range partiton file>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "ReachabilityGraph");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(LongLongLongLongVertex.class);
		job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
				getClass(), SubGraph.class);

		job.setVertexInputFormatClass(SimpleLongLongLongVertexBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongLongLongVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());

		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(),
				SimpleLongLongLongVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(),
				LongLongLongNeighborhood.class);

		job.setVertexCombinerClass(BitOrCombiner.class);

		if (Boolean.parseBoolean(args[3]))
			job.getConfiguration().setInt(
					HashMasterPartitioner.USER_PARTITION_COUNT,
					Integer.parseInt(args[4]));
		else
			MyLongRangePartitionerFactory.setRangePartitioner(job, args[4]);
		job.setWorkerConfiguration(Integer.parseInt(args[2]),
				Integer.parseInt(args[2]), 100.0f);
		if (job.run(true) == true) {
			return 0;
		} else {
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReachabilityGraph(), args));
	}

	@Override
	public void readAuxiliaryDataStructure(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeAuxiliaryDataStructure(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
