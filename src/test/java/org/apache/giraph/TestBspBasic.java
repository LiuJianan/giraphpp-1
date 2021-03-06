/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimpleCombinerVertex;
import org.apache.giraph.examples.SimpleFailVertex;
import org.apache.giraph.examples.SimpleMasterComputeVertex;
import org.apache.giraph.examples.SimpleMsgVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.examples.SimplePageRankVertex.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.lib.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.examples.SimpleSumCombiner;
import org.apache.giraph.examples.SimpleSuperstepVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.TextAggregatorWriter;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobContext;
/*if[HADOOP_NON_SASL_RPC]
else[HADOOP_NON_SASL_RPC]*/
import org.apache.hadoop.mapreduce.task.JobContextImpl;
/*end[HADOOP_NON_SASL_RPC]*/
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * Unit test for many simple BSP applications.
 */
public class TestBspBasic extends BspCase {

  public TestBspBasic() {
    super(TestBspBasic.class.getName());
  }

  /**
   * Just instantiate the vertex (all functions are implemented) and the
   * VertexInputFormat using reflection.
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws InterruptedException
   * @throws IOException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  @Test
  public void testInstantiateVertex()
      throws InstantiationException, IllegalAccessException,
      IOException, InterruptedException, IllegalArgumentException,
      InvocationTargetException, SecurityException, NoSuchMethodException {
    System.out.println("testInstantiateVertex: java.class.path=" +
        System.getProperty("java.class.path"));
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleSuperstepVertex.class,
        SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat.class);
    GraphState<LongWritable, IntWritable, FloatWritable, IntWritable> gs =
        new GraphState<LongWritable, IntWritable,
        FloatWritable, IntWritable>();
    BasicVertex<LongWritable, IntWritable, FloatWritable, IntWritable> vertex =
        BspUtils.createVertex(job.getConfiguration());
    vertex.initialize(null, null, null, null);
    System.out.println("testInstantiateVertex: Got vertex " + vertex +
        ", graphState" + gs);
    VertexInputFormat<LongWritable, IntWritable, FloatWritable, IntWritable>
    inputFormat = BspUtils.createVertexInputFormat(job.getConfiguration());
    /*if[HADOOP_NON_SASL_RPC]
      List<InputSplit> splitArray =
          inputFormat.getSplits(
              new JobContext(new Configuration(), new JobID()), 1);
    else[HADOOP_NON_SASL_RPC]*/
      List<InputSplit> splitArray =
          inputFormat.getSplits(
              new JobContextImpl(new Configuration(), new JobID()), 1);
      /*end[HADOOP_NON_SASL_RPC]*/
    ByteArrayOutputStream byteArrayOutputStream =
        new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    ((Writable) splitArray.get(0)).write(outputStream);
    System.out.println("testInstantiateVertex: Example output split = " +
        byteArrayOutputStream.toString());
  }

  /**
   * Do some checks for local job runner.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testLocalJobRunnerConfig()
      throws IOException, InterruptedException, ClassNotFoundException {
    if (runningInDistributedMode()) {
      System.out.println("testLocalJobRunnerConfig: Skipping for " +
          "non-local");
      return;
    }
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleSuperstepVertex.class, SimpleSuperstepVertexInputFormat.class);
    job.setWorkerConfiguration(5, 5, 100.0f);
    job.getConfiguration().setBoolean(GiraphJob.SPLIT_MASTER_WORKER, true);

    try {
      job.run(true);
      fail();
    } catch (IllegalArgumentException e) {
    }

    job.getConfiguration().setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
    try {
      job.run(true);
      fail();
    } catch (IllegalArgumentException e) {
    }
    job.setWorkerConfiguration(1, 1, 100.0f);
    job.run(true);
  }

  /**
   * Run a sample BSP job in JobTracker, kill a task, and make sure
   * the job fails (not enough attempts to restart)
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspFail()
      throws IOException, InterruptedException, ClassNotFoundException {
    // Allow this test only to be run on a real Hadoop setup
    if (!runningInDistributedMode()) {
      System.out.println("testBspFail: not executed for local setup.");
      return;
    }

    GiraphJob job = prepareJob(getCallingMethodName(), SimpleFailVertex.class,
        SimplePageRankVertexInputFormat.class, null,
        getTempPath(getCallingMethodName()));
    job.getConfiguration().setInt("mapred.map.max.attempts", 1);
    assertTrue(!job.run(true));
  }

  /**
   * Run a sample BSP job locally and test supersteps.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspSuperStep()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleSuperstepVertex.class, SimpleSuperstepVertexInputFormat.class,
        SimpleSuperstepVertexOutputFormat.class, outputPath);
    Configuration conf = job.getConfiguration();
    conf.setFloat(GiraphJob.TOTAL_INPUT_SPLIT_MULTIPLIER, 2.0f);
    // GeneratedInputSplit will generate 10 vertices
    conf.setLong(GeneratedVertexReader.READER_VERTICES, 10);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      FileStatus fileStatus = getSinglePartFileStatus(conf, outputPath);
      assertEquals(49l, fileStatus.getLen());
    }
  }

  /**
   * Run a sample BSP job locally and test messages.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspMsg()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = prepareJob(getCallingMethodName(), SimpleMsgVertex.class,
        SimpleSuperstepVertexInputFormat.class);
    assertTrue(job.run(true));
  }


  /**
   * Run a sample BSP job locally with no vertices and make sure
   * it completes.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testEmptyVertexInputFormat()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = prepareJob(getCallingMethodName(), SimpleMsgVertex.class,
        SimpleSuperstepVertexInputFormat.class);
    job.getConfiguration().setLong(GeneratedVertexReader.READER_VERTICES, 0);
    assertTrue(job.run(true));
  }

  /**
   * Run a sample BSP job locally with combiner and checkout output value.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspCombiner()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleCombinerVertex.class, SimpleSuperstepVertexInputFormat.class);
    job.setVertexCombinerClass(SimpleSumCombiner.class);
    assertTrue(job.run(true));
  }

  /**
   * Run a sample BSP job locally and test PageRank.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspPageRank()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimplePageRankVertex.class, SimplePageRankVertexInputFormat.class);
    job.setWorkerContextClass(
        SimplePageRankVertex.SimplePageRankVertexWorkerContext.class);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      double maxPageRank =
          SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalMax();
      double minPageRank =
          SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalMin();
      long numVertices =
          SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalSum();
      System.out.println("testBspPageRank: maxPageRank=" + maxPageRank +
          " minPageRank=" + minPageRank + " numVertices=" + numVertices);
      assertEquals(34.03, maxPageRank, 0.001);
      assertEquals(0.03, minPageRank, 0.00001);
      assertEquals(5l, numVertices);
    }
  }

  /**
   * Run a sample BSP job locally and test shortest paths.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspShortestPaths()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleShortestPathsVertex.class,
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class,
        JsonLongDoubleFloatDoubleVertexOutputFormat.class,
        outputPath);
    Configuration conf = job.getConfiguration();
    conf.setLong(SimpleShortestPathsVertex.SOURCE_ID, 0);

    assertTrue(job.run(true));

    int numResults = getNumResults(job.getConfiguration(), outputPath);

    int expectedNumResults = runningInDistributedMode() ? 15 : 5;
    assertEquals(expectedNumResults, numResults);
  }

  /**
   * Run a sample BSP job locally and test PageRank with AggregatorWriter.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspPageRankWithAggregatorWriter()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimplePageRankVertex.class,
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class,
        SimplePageRankVertex.SimplePageRankVertexOutputFormat.class,
        outputPath);
    job.setWorkerContextClass(
        SimplePageRankVertex.SimplePageRankVertexWorkerContext.class);

    Configuration conf = job.getConfiguration();

    job.setAggregatorWriterClass(TextAggregatorWriter.class);
    Path aggregatorValues = getTempPath("aggregatorValues");
    conf.setInt(TextAggregatorWriter.FREQUENCY, TextAggregatorWriter.ALWAYS);
    conf.set(TextAggregatorWriter.FILENAME, aggregatorValues.toString());

    assertTrue(job.run(true));

    FileSystem fs = FileSystem.get(conf);
    Path valuesFile = new Path(aggregatorValues.toString() + "_0");

    try {
      if (!runningInDistributedMode()) {
        double maxPageRank =
            SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalMax();
        double minPageRank =
            SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalMin();
        long numVertices =
            SimplePageRankVertex.SimplePageRankVertexWorkerContext.getFinalSum();
        System.out.println("testBspPageRank: maxPageRank=" + maxPageRank +
            " minPageRank=" + minPageRank + " numVertices=" + numVertices);

        FSDataInputStream in = null;
        BufferedReader reader = null;
        try {
          Map<Integer, Double> minValues = Maps.newHashMap();
          Map<Integer, Double> maxValues = Maps.newHashMap();
          Map<Integer, Long> vertexCounts = Maps.newHashMap();

          in = fs.open(valuesFile);
          reader = new BufferedReader(new InputStreamReader(in,
              Charsets.UTF_8));
          String line;
          while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\t");
            int superstep = Integer.parseInt(tokens[0].split("=")[1]);
            String value = (tokens[1].split("=")[1]);
            String aggregatorName = tokens[2];

            if (DoubleMinAggregator.class.getName().equals(aggregatorName)) {
              minValues.put(superstep, Double.parseDouble(value));
            }
            if (DoubleMaxAggregator.class.getName().equals(aggregatorName)) {
              maxValues.put(superstep, Double.parseDouble(value));
            }
            if (LongSumAggregator.class.getName().equals(aggregatorName)) {
              vertexCounts.put(superstep, Long.parseLong(value));
            }
          }

          int maxSuperstep = SimplePageRankVertex.MAX_SUPERSTEPS;
          assertEquals(maxSuperstep + 1, minValues.size());
          assertEquals(maxSuperstep + 1, maxValues.size());
          assertEquals(maxSuperstep + 1, vertexCounts.size());

          assertEquals(maxPageRank, maxValues.get(maxSuperstep));
          assertEquals(minPageRank, minValues.get(maxSuperstep));
          assertEquals(numVertices, vertexCounts.get(maxSuperstep));

        } finally {
          Closeables.closeQuietly(in);
          Closeables.closeQuietly(reader);
        }
      }
    } finally {
      fs.delete(valuesFile, false);
    }
  }
  
  /**
   * Run a sample BSP job locally and test MasterCompute.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspMasterCompute()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleMasterComputeVertex.class, SimplePageRankVertexInputFormat.class);
    job.setWorkerContextClass(
        SimpleMasterComputeVertex.SimpleMasterComputeWorkerContext.class);
    job.setMasterComputeClass(SimpleMasterComputeVertex.SimpleMasterCompute.class);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      double finalSum =
          SimpleMasterComputeVertex.SimpleMasterComputeWorkerContext.getFinalSum();
      System.out.println("testBspMasterCompute: finalSum=" + finalSum);
      assertEquals(32.5, finalSum);
    }
  }
}
