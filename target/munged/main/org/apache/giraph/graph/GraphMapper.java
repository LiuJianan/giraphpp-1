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

package org.apache.giraph.graph;

import com.google.common.collect.Iterables;
import com.ibm.giraph.graph.AbstractSubGraph;
import com.ibm.giraph.utils.MapRedudeUtils;

import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.Counter;

/**
 * This mapper that will execute the BSP graph tasks.  Since this mapper will
 * not be passing data by key-value pairs through the MR framework, the
 * types are irrelevant.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class GraphMapper<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable> extends
    Mapper<Object, Object, Object, Object> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GraphMapper.class);
  /** Coordination service worker */
  private CentralizedServiceWorker<I, V, E, M> serviceWorker;
  /** Coordination service master */
  private CentralizedServiceMaster<I, V, E, M> serviceMaster;
  /** Coordination service master thread */
  private Thread masterThread = null;
  /** The map should be run exactly once, or else there is a problem. */
  private boolean mapAlreadyRun = false;
  /** Manages the ZooKeeper servers if necessary (dynamic startup) */
  private ZooKeeperManager zkManager;
  /** Configuration */
  private Configuration conf;
  /** Already complete? */
  private boolean done = false;
  /** What kind of functions is this mapper doing? */
  private MapFunctions mapFunctions = MapFunctions.UNKNOWN;
  /**
   * Graph state for all vertices that is used for the duration of
   * this mapper.
   */
  private GraphState<I, V, E, M> graphState = new GraphState<I, V, E, M>();

  //added code to support GiraphPlusPlus
  private List<M> msgBuffer=new ArrayList<M>(2);
  boolean toKill=false;
  Counter checkpointWriteCounter= null;
  Counter checkpointReadCounter= null;
  /** How are Subgraphs present on one worker handled? */
  private Class<? extends AbstractSubGraph<I, V, E, M>> subGraphManagerClass=null;
  private AbstractSubGraph<I, V, E, M> subGraphManager=null;

	public boolean isInSubGraphMode()
	{
		return subGraphManagerClass!=null;
	}
	
	public AbstractSubGraph<I, V, E, M> createNewSubGraph()
	{
		if(subGraphManagerClass==null)
			return null;
		try {
			return (AbstractSubGraph<I, V, E, M>) subGraphManagerClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}
	
	public AbstractSubGraph<I, V, E, M> getSubGraph()
	{
		return subGraphManager;
	}
	
	public void setSubGraph(AbstractSubGraph<I, V, E, M> g)
	{
		subGraphManager=g;
	}
	
	/** indicate the number of iterations run per super steps, apply the internal messages and cache the out messages*/
	private int substeps=-1;
	
	/** Map of local Messages to be processed within one Superstep */
	//private HashMap<I,List<M>> localMessages = new HashMap<I,List<M>>();
	
	/** Senderside combiner **/
	private VertexCombiner<I, M> sendersideCombiner = null;
	
	public VertexCombiner<I, M> getSenderSideCombiner()
	{
		return sendersideCombiner;
	}
	
	/** Formats the Superstep Counter **/
	private DecimalFormat counterformat = new DecimalFormat  ( ",000" );
	
	/** indicate whether to distinguish local messges from remote messages */

  
  /** What kinds of functions to run on this mapper */
  public enum MapFunctions {
    /** Undecided yet */
    UNKNOWN,
    /** Only be the master */
    MASTER_ONLY,
    /** Only be the master and ZooKeeper */
    MASTER_ZOOKEEPER_ONLY,
    /** Only be the worker */
    WORKER_ONLY,
    /** Do master, worker, and ZooKeeper */
    ALL,
    /** Do master and worker */
    ALL_EXCEPT_ZOOKEEPER
  }

  /**
   * Get the map function enum.
   *
   * @return Map functions of this mapper.
   */
  public MapFunctions getMapFunctions() {
    return mapFunctions;
  }

  /**
   * Get the aggregator usage, a subset of the functionality
   *
   * @return Aggregator usage interface
   */
  public final AggregatorUsage getAggregatorUsage() {
    AggregatorUsage result = null;
    if (serviceWorker != null) {
      result = serviceWorker;
    }
    if (serviceMaster != null) {
      result = serviceMaster;
    }
    return result;
  }

  public final WorkerContext getWorkerContext() {
    return serviceWorker.getWorkerContext();
  }

  public final GraphState<I, V, E, M> getGraphState() {
    return graphState;
  }

  /**
   * Default handler for uncaught exceptions.
   */
  class OverrideExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.fatal(
          "uncaughtException: OverrideExceptionHandler on thread " +
              t.getName() + ", msg = " +  e.getMessage() + ", exiting...", e);
      System.exit(1);
    }
  }

  /**
   * Copied from JobConf to get the location of this jar.  Workaround for
   * things like Oozie map-reduce jobs.
   *
   * @param myClass Class to search the class loader path for to locate
   *        the relevant jar file
   * @return Location of the jar file containing myClass
   */
  private static String findContainingJar(Class<?> myClass) {
    ClassLoader loader = myClass.getClassLoader();
    String classFile =
        myClass.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<?> itr = loader.getResources(classFile);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  /**
   * Make sure that all registered classes have matching types.  This
   * is a little tricky due to type erasure, cannot simply get them from
   * the class type arguments.  Also, set the vertex index, vertex value,
   * edge value and message value classes.
   *
   * @param conf Configuration to get the various classes
   */
  public void determineClassTypes(Configuration conf) {
    Class<? extends BasicVertex<I, V, E, M>> vertexClass =
        BspUtils.<I, V, E, M>getVertexClass(conf);
    List<Class<?>> classList = ReflectionUtils.<BasicVertex>getTypeArguments(
        BasicVertex.class, vertexClass);
    Type vertexIndexType = classList.get(0);
    Type vertexValueType = classList.get(1);
    Type edgeValueType = classList.get(2);
    Type messageValueType = classList.get(3);

    Class<? extends VertexInputFormat<I, V, E, M>> vertexInputFormatClass =
        BspUtils.<I, V, E, M>getVertexInputFormatClass(conf);
    classList = ReflectionUtils.<VertexInputFormat>getTypeArguments(
        VertexInputFormat.class, vertexInputFormatClass);
    if (classList.get(0) == null) {
      LOG.warn("Input format vertex index type is not known");
    } else if (!vertexIndexType.equals(classList.get(0))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Vertex index types don't match, " +
              "vertex - " + vertexIndexType +
              ", vertex input format - " + classList.get(0));
    }
    if (classList.get(1) == null) {
      LOG.warn("Input format vertex value type is not known");
    } else if (!vertexValueType.equals(classList.get(1))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Vertex value types don't match, " +
              "vertex - " + vertexValueType +
              ", vertex input format - " + classList.get(1));
    }
    if (classList.get(2) == null) {
      LOG.warn("Input format edge value type is not known");
    } else if (!edgeValueType.equals(classList.get(2))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Edge value types don't match, " +
              "vertex - " + edgeValueType +
              ", vertex input format - " + classList.get(2));
    }
    // If has vertex combiner class, check
    Class<? extends VertexCombiner<I, M>> vertexCombinerClass =
        BspUtils.<I, M>getVertexCombinerClass(conf);
    if (vertexCombinerClass != null) {
      classList = ReflectionUtils.<VertexCombiner>getTypeArguments(
          VertexCombiner.class, vertexCombinerClass);
      if (!vertexIndexType.equals(classList.get(0))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Vertex index types don't match, " +
                "vertex - " + vertexIndexType +
                ", vertex combiner - " + classList.get(0));
      }
      if (!messageValueType.equals(classList.get(1))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Message value types don't match, " +
                "vertex - " + vertexValueType +
                ", vertex combiner - " + classList.get(1));
      }
    }
    // If has vertex output format class, check
    Class<? extends VertexOutputFormat<I, V, E>>
    vertexOutputFormatClass =
      BspUtils.<I, V, E>getVertexOutputFormatClass(conf);
    if (vertexOutputFormatClass != null) {
      classList =
          ReflectionUtils.<VertexOutputFormat>getTypeArguments(
              VertexOutputFormat.class, vertexOutputFormatClass);
      if (classList.get(0) == null) {
        LOG.warn("Output format vertex index type is not known");
      } else if (!vertexIndexType.equals(classList.get(0))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Vertex index types don't match, " +
                "vertex - " + vertexIndexType +
                ", vertex output format - " + classList.get(0));
      }
      if (classList.get(1) == null) {
        LOG.warn("Output format vertex value type is not known");
      } else if (!vertexValueType.equals(classList.get(1))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Vertex value types don't match, " +
                "vertex - " + vertexValueType +
                ", vertex output format - " + classList.get(1));
      }
      if (classList.get(2) == null) {
        LOG.warn("Output format edge value type is not known");
      } else if (!edgeValueType.equals(classList.get(2))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Edge value types don't match, " +
                "vertex - " + vertexIndexType +
                ", vertex output format - " + classList.get(2));
      }
    }
    // Vertex resolver might never select the types
    Class<? extends VertexResolver<I, V, E, M>>
    vertexResolverClass =
      BspUtils.<I, V, E, M>getVertexResolverClass(conf);
    classList = ReflectionUtils.<VertexResolver>getTypeArguments(
        VertexResolver.class, vertexResolverClass);
    if (classList.get(0) != null &&
        !vertexIndexType.equals(classList.get(0))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Vertex index types don't match, " +
              "vertex - " + vertexIndexType +
              ", vertex resolver - " + classList.get(0));
    }
    if (classList.get(1) != null &&
        !vertexValueType.equals(classList.get(1))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Vertex value types don't match, " +
              "vertex - " + vertexValueType +
              ", vertex resolver - " + classList.get(1));
    }
    if (classList.get(2) != null &&
        !edgeValueType.equals(classList.get(2))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Edge value types don't match, " +
              "vertex - " + edgeValueType +
              ", vertex resolver - " + classList.get(2));
    }
    if (classList.get(3) != null &&
        !messageValueType.equals(classList.get(3))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Message value types don't match, " +
              "vertex - " + edgeValueType +
              ", vertex resolver - " + classList.get(3));
    }
    conf.setClass(GiraphJob.VERTEX_INDEX_CLASS,
        (Class<?>) vertexIndexType,
        WritableComparable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS,
        (Class<?>) vertexValueType,
        Writable.class);
    conf.setClass(GiraphJob.EDGE_VALUE_CLASS,
        (Class<?>) edgeValueType,
        Writable.class);
    conf.setClass(GiraphJob.MESSAGE_VALUE_CLASS,
        (Class<?>) messageValueType,
        Writable.class);
  }

  /**
   * Figure out what functions this mapper should do.  Basic logic is as
   * follows:
   * 1) If not split master, everyone does the everything and/or running
   *    ZooKeeper.
   * 2) If split master/worker, masters also run ZooKeeper (if it's not
   *    given to us).
   *
   * @param conf Configuration to use
   * @param zkManager ZooKeeper manager to help determine whether to run
   *        ZooKeeper
   * @return Functions that this mapper should do.
   */
  private static MapFunctions determineMapFunctions(
      Configuration conf,
      ZooKeeperManager zkManager) {
    boolean splitMasterWorker =
        conf.getBoolean(GiraphJob.SPLIT_MASTER_WORKER,
            GiraphJob.SPLIT_MASTER_WORKER_DEFAULT);
    int taskPartition = conf.getInt("mapred.task.partition", -1);
    boolean zkAlreadyProvided =
        conf.get(GiraphJob.ZOOKEEPER_LIST) != null;
    MapFunctions functions = MapFunctions.UNKNOWN;
    // What functions should this mapper do?
    if (!splitMasterWorker) {
      if ((zkManager != null) && zkManager.runsZooKeeper()) {
        functions = MapFunctions.ALL;
      } else {
        functions = MapFunctions.ALL_EXCEPT_ZOOKEEPER;
      }
    } else {
      if (zkAlreadyProvided) {
        int masterCount =
            conf.getInt(GiraphJob.ZOOKEEPER_SERVER_COUNT,
                GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
        if (taskPartition < masterCount) {
          functions = MapFunctions.MASTER_ONLY;
        } else {
          functions = MapFunctions.WORKER_ONLY;
        }
      } else {
        if ((zkManager != null) && zkManager.runsZooKeeper()) {
          functions = MapFunctions.MASTER_ZOOKEEPER_ONLY;
        } else {
          functions = MapFunctions.WORKER_ONLY;
        }
      }
    }
    return functions;
  }

  @Override
  public void setup(Context context)
    throws IOException, InterruptedException {
    context.setStatus("setup: Beginning mapper setup.");
    graphState.setContext(context);
    // Setting the default handler for uncaught exceptions.
    Thread.setDefaultUncaughtExceptionHandler(
        new OverrideExceptionHandler());
    conf = context.getConfiguration();
    // Hadoop security needs this property to be set
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      conf.set("mapreduce.job.credentials.binary",
          System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }
    // Ensure the user classes have matching types and figure them out
    determineClassTypes(conf);

    checkpointWriteCounter= context.getCounter("CheckPoint", "Write");
    checkpointReadCounter= context.getCounter("CheckPoint", "Read");
    
    // Check if SubGraphManager is specified
    subGraphManagerClass = (Class<? extends AbstractSubGraph<I, V, E, M>>) conf.getClass(GiraphJob.SUBGRAPH_MANAGER_CLASS, null);
    if (subGraphManagerClass == null){
    	subGraphManager = null;
    	substeps=conf.getInt(GiraphJob.NUM_SUB_STEPS_PER_ITERATION, -1);
    }else
    {
		try{
			subGraphManager = (AbstractSubGraph<I, V, E, M>) subGraphManagerClass.newInstance();
			if (LOG.isInfoEnabled()){
    			LOG.info("setup: Using SubGraphManager: " + subGraphManagerClass.getCanonicalName());
    		}
		}
		catch (Exception e){
			LOG.error("setup: The SubGraphManager failed to instanciate",e);
		}
    }
    if (BspUtils.getVertexCombinerClass(conf) == null) {
        this.sendersideCombiner = null;
    } else {
    	this.sendersideCombiner = BspUtils.createVertexCombiner(conf);
    }    
    // Do some initial setup (possibly starting up a Zookeeper service)
    context.setStatus("setup: Initializing Zookeeper services.");
    if (!conf.getBoolean(GiraphJob.LOCAL_TEST_MODE,
        GiraphJob.LOCAL_TEST_MODE_DEFAULT)) {
      Path[] fileClassPaths = DistributedCache.getLocalCacheArchives(conf);
      String zkClasspath = null;
      if (fileClassPaths == null) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Distributed cache is empty. Assuming fatjar.");
        }
        String jarFile = context.getJar();
        if (jarFile == null) {
          jarFile = findContainingJar(getClass());
        }
        zkClasspath = jarFile.replaceFirst("file:", "");
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append(fileClassPaths[0]);

        for (int i = 1; i < fileClassPaths.length; i++) {
          sb.append(":");
          sb.append(fileClassPaths[i]);
        }
        zkClasspath = sb.toString();
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("setup: classpath @ " + zkClasspath);
      }
      conf.set(GiraphJob.ZOOKEEPER_JAR, zkClasspath);
    }
    String serverPortList =
        conf.get(GiraphJob.ZOOKEEPER_LIST, "");
    if (serverPortList.isEmpty()) {
      zkManager = new ZooKeeperManager(context);
      context.setStatus("setup: Setting up Zookeeper manager.");
      zkManager.setup();
      if (zkManager.computationDone()) {
        done = true;
        return;
      }
      zkManager.onlineZooKeeperServers();
      serverPortList = zkManager.getZooKeeperServerPortString();
    }
    context.setStatus("setup: Connected to Zookeeper service " +
        serverPortList);
    this.mapFunctions = determineMapFunctions(conf, zkManager);

    // Sometimes it takes a while to get multiple ZooKeeper servers up
    if (conf.getInt(GiraphJob.ZOOKEEPER_SERVER_COUNT,
        GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT) > 1) {
      Thread.sleep(GiraphJob.DEFAULT_ZOOKEEPER_INIT_LIMIT *
          GiraphJob.DEFAULT_ZOOKEEPER_TICK_TIME);
    }
    int sessionMsecTimeout =
        conf.getInt(GiraphJob.ZOOKEEPER_SESSION_TIMEOUT,
            GiraphJob.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
    try {
      if ((mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
          (mapFunctions == MapFunctions.MASTER_ONLY) ||
          (mapFunctions == MapFunctions.ALL) ||
          (mapFunctions == MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Starting up BspServiceMaster " +
              "(master thread)...");
        }
        serviceMaster = new BspServiceMaster<I, V, E, M>(serverPortList,
                sessionMsecTimeout,
                context,
                this);
        masterThread = new MasterThread<I, V, E, M>(
                (BspServiceMaster<I, V, E, M>) serviceMaster, context);
        masterThread.start();
      }
      if ((mapFunctions == MapFunctions.WORKER_ONLY) ||
          (mapFunctions == MapFunctions.ALL) ||
          (mapFunctions == MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Starting up BspServiceWorker...");
        }
        serviceWorker = new BspServiceWorker<I, V, E, M>(
            serverPortList,
            sessionMsecTimeout,
            context,
            this,
            graphState);
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Registering health of this worker...");
        }
        serviceWorker.setup();
      }
    } catch (IOException e) {
      LOG.error("setup: Caught exception just before end of setup", e);
      if (zkManager != null) {
        zkManager.offlineZooKeeperServers(
            ZooKeeperManager.State.FAILED);
      }
      throw new RuntimeException(
          "setup: Offlining servers due to exception...", e);
    }
    context.setStatus(getMapFunctions().toString() + " starting...");
  }

  @Override
  public void map(Object key, Object value, Context context)
    throws IOException, InterruptedException {
	  
    // map() only does computation
    // 1) Run checkpoint per frequency policy.
    // 2) For every vertex on this mapper, run the compute() function
    // 3) Wait until all messaging is done.
    // 4) Check if all vertices are done.  If not goto 2).
    // 5) Dump output.
    if (done) {
      return;
    }
    if ((serviceWorker != null) && (graphState.getNumVertices() == 0)) {
      return;
    }

    if ((mapFunctions == MapFunctions.MASTER_ZOOKEEPER_ONLY) ||
        (mapFunctions == MapFunctions.MASTER_ONLY)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("map: No need to do anything when not a worker");
      }
      return;
    }

    if (mapAlreadyRun) {
      throw new RuntimeException("In BSP, map should have only been" +
          " run exactly once, (already run)");
    }
    mapAlreadyRun = true;
	  
    graphState.setSuperstep(serviceWorker.getSuperstep()).
      setContext(context).setGraphMapper(this);

    try {
      serviceWorker.getWorkerContext().preApplication();
    } catch (InstantiationException e) {
      LOG.fatal("map: preApplication failed in instantiation", e);
      throw new RuntimeException(
          "map: preApplication failed in instantiation", e);
    } catch (IllegalAccessException e) {
      LOG.fatal("map: preApplication failed in access", e);
      throw new RuntimeException(
          "map: preApplication failed in access", e);
    }
    context.progress();

    List<PartitionStats> partitionStatsList =
        new ArrayList<PartitionStats>();
    do {
      long superstep = serviceWorker.getSuperstep();
      graphState.setSuperstep(superstep);
      
      Collection<? extends PartitionOwner> masterAssignedPartitionOwners =
          serviceWorker.startSuperstep();
      if (zkManager != null && zkManager.runsZooKeeper()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("map: Chosen to run ZooKeeper...");
        }
        context.setStatus("map: Running Zookeeper Server");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("map: " + MemoryUtils.getRuntimeMemoryStats());
      }
      context.progress();

      serviceWorker.exchangeVertexPartitions(
          masterAssignedPartitionOwners);
      context.progress();

      // Might need to restart from another superstep
      // (manually or automatic), or store a checkpoint
      if (serviceWorker.getRestartedSuperstep() == superstep) {
        if (LOG.isInfoEnabled()) {
          LOG.info("map: Loading from checkpoint " + superstep);
        }
        //record checkpointing time and recovery time
        long time=System.currentTimeMillis();
        serviceWorker.loadCheckpoint(
            serviceWorker.getRestartedSuperstep());
        time=System.currentTimeMillis()-time;
        checkpointReadCounter.increment(time);
        
      } else if (serviceWorker.checkpointFrequencyMet(superstep)) {
    	long time=System.currentTimeMillis();
        serviceWorker.storeCheckpoint();
        time=System.currentTimeMillis()-time;
        checkpointWriteCounter.increment(time);
      }

      serviceWorker.getWorkerContext().setGraphState(graphState);
      serviceWorker.getWorkerContext().preSuperstep();
      context.progress();

      partitionStatsList.clear();
      
      for (Partition<I, V, E, M> partition :
        serviceWorker.getPartitionMap().values()) {
    
	  LOG.info("### start processing partition: "+partition.getPartitionId()
  			+" with "+partition.getVertices().size()+" vertices and "
  			+partition.getEdgeCount()+" edges");
	  
	  PartitionStats partitionStats =
        new PartitionStats(partition.getPartitionId(), 0, 0, 0);
    	  
      	if(subGraphManager==null)
      	{
      		if(substeps<=0)//basic vertex-centric model
      		{
      			graphState.setCurrentPartition(null);
      			for (BasicVertex<I, V, E, M> basicVertex :
                      partition.getVertices()) {
	                    // Make sure every vertex has the current
	                    // graphState before computing
	                    basicVertex.setGraphState(graphState);
	                    if (basicVertex.isHalted()
	                            && !Iterables.isEmpty(basicVertex.getMessages())) {
	                        basicVertex.halt = false;
	                    }
	                    if (!basicVertex.isHalted()) {
	                        Iterator<M> vertexMsgIt =
	                            basicVertex.getMessages().iterator();
	                        context.progress();
	                        basicVertex.compute(vertexMsgIt);
	                        basicVertex.releaseResources();
	                    }
	                    if (basicVertex.isHalted()) {
	                        partitionStats.incrFinishedVertexCount();
	                    }
	                    partitionStats.incrVertexCount();
	                    partitionStats.addEdgeCount(basicVertex.getNumOutEdges());
	                }
      		}else// the hybrid model
      		{
      			partition.setCombiner(sendersideCombiner);
      			graphState.setCurrentPartition(partition);
      			for (Entry<I, BasicVertex<I, V, E, M>> mapEntry : partition.getVerticesMap().entrySet()) {
	                    // Make sure every vertex has the current
	                    // graphState before computing
      					I id=mapEntry.getKey();
      					BasicVertex<I, V, E, M> basicVertex=mapEntry.getValue();
	                    basicVertex.setGraphState(graphState);
	                    List<M> msgList=partition.getInternalMessages(id);
	                    if (basicVertex.isHalted()
	                        && ( !Iterables.isEmpty(basicVertex.getMessages()) || (msgList!=null && !msgList.isEmpty()) ))
	                    {
	                        basicVertex.halt = false;
	                    }
	                    if (!basicVertex.isHalted()) {
	                    	
	                    	Iterator<M> vertexMsgIt =basicVertex.getMessages().iterator();
		                    context.progress();
	                    	if(msgList==null || msgList.isEmpty())
	                    	{ 
		                        basicVertex.compute(vertexMsgIt);
	                    	}else
	                    	{
	                    		//this can also handel self-loop
	                    		msgBuffer.clear();
	                    		msgBuffer.addAll(msgList);
	                    		msgList.clear();
	                    		while(vertexMsgIt.hasNext())
	                    			msgBuffer.add(vertexMsgIt.next());
	                    		basicVertex.compute(msgBuffer.iterator());
	                    	}
	                        basicVertex.releaseResources();
	                    }
	                    partitionStats.incrVertexCount();
	                    partitionStats.addEdgeCount(basicVertex.getNumOutEdges());
	            }
      			
      			for (Entry<I, BasicVertex<I, V, E, M>> mapEntry : partition.getVerticesMap().entrySet()) {
  					List<M> msgList=partition.getInternalMessages(mapEntry.getKey());
      				 if (mapEntry.getValue().isHalted() && (msgList==null || msgList.isEmpty())) {
	                        partitionStats.incrFinishedVertexCount();
	                 }else
	                	 mapEntry.getValue().halt=false;
      			}
      		}
      		
      	}else
      	{
      		if (LOG.isInfoEnabled())
          		LOG.info("Using SubGraphManager");
      		//TODO: this for loop can be removed, since graphState is in the partition now
      		for (BasicVertex<I, V, E, M> basicVertex :
                  partition.getVertices()) {
                  // Make sure every vertex has the current
                  // graphState before computing
                  basicVertex.setGraphState(graphState);
      		}
          	
          	subGraphManager.compute(partition, context, sendersideCombiner, graphState);
          	if (LOG.isInfoEnabled())
          		LOG.info("SubGraphManager finished");
          	context.progress();
          	
          	for (BasicVertex<I, V, E, M> basicVertex : partition.getVertices())
          	{
          		basicVertex.releaseResources();
          		if (basicVertex.isHalted()) {
                      partitionStats.incrFinishedVertexCount();
                  }
                  partitionStats.incrVertexCount();
                  partitionStats.addEdgeCount(basicVertex.getNumOutEdges());
          		
          	}
      	}
      	partitionStatsList.add(partitionStats);
      }
      
      serviceWorker.getWorkerContext().postSuperstep();
           
    } while (!serviceWorker.finishSuperstep(partitionStatsList));
    if (LOG.isInfoEnabled()) {
      LOG.info("map: BSP application done " +
          "(global vertices marked done)");
    }

    serviceWorker.getWorkerContext().postApplication();
    context.progress();
  }

  @Override
  public void cleanup(Context context)
    throws IOException, InterruptedException {
    if (LOG.isInfoEnabled()) {
      LOG.info("cleanup: Starting for " + getMapFunctions());
    }
    if (done) {
      return;
    }

    if (serviceWorker != null) {
      serviceWorker.cleanup();
    }
    try {
      if (masterThread != null) {
        masterThread.join();
      }
    } catch (InterruptedException e) {
      // cleanup phase -- just log the error
      LOG.error("cleanup: Master thread couldn't join");
    }
    if (zkManager != null) {
      zkManager.offlineZooKeeperServers(
          ZooKeeperManager.State.FINISHED);
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    // Notify the master quicker if there is worker failure rather than
    // waiting for ZooKeeper to timeout and delete the ephemeral znodes
    try {
      setup(context);
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(),
            context.getCurrentValue(),
            context);
      }
      cleanup(context);
    } catch (IOException e) {
      if (mapFunctions == MapFunctions.WORKER_ONLY) {
        serviceWorker.failureCleanup();
      }
      throw new IllegalStateException(
          "run: Caught an unrecoverable exception " + e.getMessage(), e);
    }
  }
}
