package com.ibm.giraph.graph.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.LongDoubleFloatDoubleVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.map.OpenLongFloatHashMap;

public class BetterLongDoubleFloatDoubleVertex extends LongDoubleFloatDoubleVertex{

	    public final long getVertexIdSimple() {
	        return vertexId;
	    }

	    public final double getVertexValueSimple() {
	        return vertexValue;
	    }

	    public final void setVertexValue(double vertexValue) {
	        this.vertexValue = vertexValue;
	    }

	    public float getEdgeValue(long targetVertexId) {
	        return verticesWithEdgeValues.get(targetVertexId);
	    }
	    
	    public DoubleArrayList getMessagesList()
	    {
	    	return messageList;
	    }

		@Override
		public void compute(Iterator<DoubleWritable> msgIterator)
				throws IOException {
			// do nothing
			
		}
		
		public OpenLongFloatHashMap getEdges()
		{
			return verticesWithEdgeValues;
		}
}
