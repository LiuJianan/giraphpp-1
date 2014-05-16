package com.ibm.giraph.graph.example.ioformats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;
import com.ibm.giraph.graph.example.coarsen.CoarsenVertexValue;

public class SimpleLongCoarsenVertexValueLongMVertexBinaryInputFormat <M extends Writable> 
extends VertexInputFormat<LongWritable, CoarsenVertexValue, LongWritable, M>{

	private KVBinaryInputFormat<SkeletonNeighborhood> inputFormat= new KVBinaryInputFormat<SkeletonNeighborhood>(); 
	
	class SimpleVertexBinaryReader<M extends Writable> 
	extends SimpleVertexBinaryReaderBase<CoarsenVertexValue, LongWritable, M, SkeletonNeighborhood>
	{
		public SimpleVertexBinaryReader(
				RecordReader<LongWritable, SkeletonNeighborhood> basereader) {
			super(basereader);
		}

		@Override
		public BasicVertex<LongWritable, CoarsenVertexValue, LongWritable, M> getCurrentVertex()
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
		    BasicVertex<LongWritable, CoarsenVertexValue, LongWritable, M> vertex = BspUtils.createVertex(conf);

		    LongWritable vertexId = new LongWritable(reader.getCurrentKey().get());
		    SkeletonNeighborhood value=reader.getCurrentValue();
		    CoarsenVertexValue vertexValue=new CoarsenVertexValue((byte)1, 1);
		    
		    int n=value.getNumberEdges();
		    Map<LongWritable, LongWritable> edges = Maps.newHashMap();
			for(int i=0; i<n; i++)
				edges.put(new LongWritable(value.getEdgeID(i)), 
						new LongWritable(1));
		    vertex.initialize(vertexId, vertexValue, edges, null);
		    return vertex;
		}	
	}

	@Override
	public List<InputSplit> getSplits(JobContext context, int numWorkers)
			throws IOException, InterruptedException {
		return inputFormat.getSplits(context);
	}

	@Override
	public VertexReader<LongWritable, CoarsenVertexValue, LongWritable, M> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new SimpleVertexBinaryReader(inputFormat.createRecordReader(split, context));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
