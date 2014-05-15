package com.ibm.giraph.graph.example.reachability;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.graph.DefaultMasterCompute;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongLongLongLongVertex;
import org.apache.giraph.graph.LongLongLongNeighborhood;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongDoubleNullXVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongLongLongVertexBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.SimpleLongXXXBinaryVertexOutputFormat;
import com.ibm.giraph.graph.example.partitioners.MyLongRangePartitionerFactory;
import com.ibm.giraph.utils.MapRedudeUtils;

public class ReachabilityVertex extends LongLongLongLongVertex implements Tool {

	private static String BFS_AGG = "star";
	public static int BFS_SOURCE = 0;
	public static int BFS_DEST = 0;

	Configuration conf = null;
	/** Logger */
	private static final Logger LOG = Logger
			.getLogger(ReachabilityVertex.class);

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReachabilityVertex(), args));
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
		BooleanOrAggregator agg  = (BooleanOrAggregator) getAggregator(ReachabilityVertex.BFS_AGG);
		if(agg.getAggregatedValue().get())
		{
			voteToHalt();
			return;
		}
		if (getSuperstep() == 0) {
			long vid = this.getVertexId().get();
			long tag = 0;
			if (vid == BFS_SOURCE)
				tag = 1;
			else if (vid == BFS_DEST)
				tag = 2;

			this.setVertexValue(new LongWritable(tag));

			if (tag == 2) // v->dest
			{
				for (int i = 0; i < this.getNumOutEdges(); i++) {
					if (getSimpleEdgeValue(i) == 1) // in edge
					{
						sendMsg(this.getEdgeID(i), new LongWritable(tag));
					}
				}

			} else if (tag == 1) {
				for (int i = 0; i < this.getNumOutEdges(); i++) {
					if (getSimpleEdgeValue(i) == 0) // out edge
					{
						sendMsg(this.getEdgeID(i), new LongWritable(tag));
					}
				}
			}

			
		} else {
			long tag = 0;
			while (msgs.hasNext()) {
				tag |= msgs.next().get();
			}
			long mytag = this.getVertexValueSimpleType();
			
			if ((tag | mytag) != mytag) {
				mytag |= tag;
				this.setVertexValue(new LongWritable(mytag));
				if (mytag == 3) {
					agg.aggregate(true);
				} else if (tag == 2) // v->dst
				{
					for (int i = 0; i < this.getNumOutEdges(); i++) {
						if (getSimpleEdgeValue(i) == 1) // in edge
						{
							sendMsg(this.getEdgeID(i), new LongWritable(mytag));
						}
					}
				} else if (tag == 1) // src->v
				{
					for (int i = 0; i < this.getNumOutEdges(); i++) {
						if (getSimpleEdgeValue(i) == 0) // out edge
						{
							sendMsg(this.getEdgeID(i), new LongWritable(mytag));
						}
					}
				}
			}
			
		}
		voteToHalt();
	}
	
	public static class ReachabilityWorkerContext extends WorkerContext {

		private static boolean FINAL_BFS;

		public static boolean getFinalStar() {
			return FINAL_BFS;
		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
		}

		@Override
		public void postApplication() {

			BooleanOrAggregator agg  = (BooleanOrAggregator) getAggregator(ReachabilityVertex.BFS_AGG);
			FINAL_BFS = agg.getAggregatedValue().get();
		}

		@Override
		public void preSuperstep() {

		}

		@Override
		public void postSuperstep() {
		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class ReachabilityMasterCompute extends
			DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(BFS_AGG, BooleanOrAggregator.class);
		}
	}
	

	public static class SimpleLongLongLongVertexBinaryOutputFormat
			extends
			SimpleLongXXXBinaryVertexOutputFormat<LongWritable, LongWritable, LongLongLongNeighborhood<LongWritable, LongWritable>> {

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 8) {
			System.err
					.println("run: Must have 8 arguments <input path> <output path> <# of workers> <hash partition: true, range partition: false> <BFS_SOURCE> <BFS_DEST>"
							+ "<if hash partition, #partitions, otherwise range partiton file> <hybrid model: true, otherwise: false>");
			System.exit(-1);
		}
		GiraphJob job = new GiraphJob(getConf(), "ReachabilityVertex");
		job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
		job.setVertexClass(ReachabilityVertex.class);
		job.setVertexInputFormatClass(SimpleLongLongLongVertexBinaryInputFormat.class);
		job.setVertexOutputFormatClass(SimpleLongLongLongVertexBinaryOutputFormat.class);
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job.getInternalJob(), outpath);
		MapRedudeUtils.deleteFileIfExistOnHDFS(outpath, job.getConfiguration());

		KVBinaryInputFormat
				.setInputNeighborhoodClass(
						job.getConfiguration(),
						SimpleLongDoubleNullXVertexBinaryInputFormat.NEIGHBORHOOD_CLASS);
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
		if (Boolean.parseBoolean(args[5]))
			job.getConfiguration().setInt(
					GiraphJob.NUM_SUB_STEPS_PER_ITERATION, 1);
		else
			job.getConfiguration().setInt(
					GiraphJob.NUM_SUB_STEPS_PER_ITERATION, 0);

		ReachabilityVertex.BFS_SOURCE = Integer.parseInt(args[6]);
		ReachabilityVertex.BFS_DEST = Integer.parseInt(args[7]);

		if (job.run(true) == true) {
			return 0;
		} else {
			return -1;
		}
	}
}
