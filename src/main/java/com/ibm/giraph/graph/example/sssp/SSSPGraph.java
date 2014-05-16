package com.ibm.giraph.graph.example.sssp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongDoubleDoubleDoubleVertex;
import org.apache.giraph.graph.LongDoubleDoubleNeighborhood;

import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.graph.SubGraph;
import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongDoubleDoubleVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.graph.example.sssp.SSSPVertex.SimpleLongDoubleDoubleVertexBinaryOutputFormat;
import com.ibm.giraph.utils.MapRedudeUtils;

public class SSSPGraph
		extends
		AbstractSubGraph<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable>
		implements Tool {

	private static final Logger LOG = Logger.getLogger(SSSPGraph.class);

	@Override
	public void compute(
			final Partition<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> subgraph)
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
		job.setVertexClass(LongDoubleDoubleDoubleVertex.class);
		job.getConfiguration().setClass(GiraphJob.SUBGRAPH_MANAGER_CLASS,
				getClass(), SubGraph.class);

		job.setVertexInputFormatClass(SimpleLongDoubleDoubleVertexBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongDoubleDoubleVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());

		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), SimpleLongDoubleDoubleVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
		KVBinaryOutputFormat.setOutputNeighborhoodClass(job.getConfiguration(), LongDoubleDoubleNeighborhood.class);

		job.setVertexCombinerClass(MinCombiner.class);

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
		System.exit(ToolRunner.run(new SSSPGraph(), args));
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

