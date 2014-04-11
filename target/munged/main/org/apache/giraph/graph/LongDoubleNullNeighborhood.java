package org.apache.giraph.graph;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.ibm.giraph.graph.example.ioformats.Neighborhood;


public class LongDoubleNullNeighborhood implements Neighborhood<LongWritable, DoubleWritable, NullWritable>
{
	protected double vertexValue;
	protected long[] edgeIDs;
	protected int n;
	
	public String toString()
	{
		String str="vertexValue: "+vertexValue+" numEdges: "+n+"\n";
		for(int i=0; i<n; i++)
			str+=edgeIDs[i]+", ";
		str+="\n";
		return str;
	}
	
	public void setVertexValue(double v)
	{
		vertexValue=v;
	}
	
	public void setEdges(Collection<Edge<LongWritable, NullWritable>> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Edge<LongWritable, NullWritable> e: edges)
		{
			edgeIDs[i]=e.getDestVertexId().get();
			i++;
		}
	}
	
	public void setSimpleEdges(Collection<Long> edges)
	{
		setSize(edges.size());
		int i=0;
		for(Long e: edges)
		{
			edgeIDs[i]=e;
			i++;
		}
	}
	
	public void set(BasicVertex<LongWritable, DoubleWritable, NullWritable, ?> vertex)
	{
		vertexValue=vertex.getVertexValue().get();
		setSize(vertex.getNumOutEdges());
		int i=0;
		for(LongWritable v: vertex)
		{
			edgeIDs[i]=v.get();
			i++;
		}
	}
	
	public double getVertexValue()
	{
		return vertexValue;
	}
	
	public int getNumberEdges()
	{
		return n;
	}
	
	public long getEdgeID(int i)
	{
		if(i>=n || i<0) throw new RuntimeException("index "+i+" is not in range [0, "+n+")");
		return edgeIDs[i];
	}
	
	private void setSize(int s)
	{
		n=s;
		if(edgeIDs==null || edgeIDs.length<n)
		{
			edgeIDs=new long[n];
		}
	}
	
	public LongDoubleNullNeighborhood()
	{
		n=0;
		edgeIDs=null;
	}
	
	public LongDoubleNullNeighborhood(int capacity)
	{
		n=0;
		edgeIDs=new long[capacity];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexValue=in.readDouble();
		setSize(in.readInt());
		for(int i=0; i<n; i++)
		{
			edgeIDs[i]=in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(vertexValue);
		out.writeInt(n);
		for(int i=0; i<n; i++)
		{
			out.writeLong(edgeIDs[i]);
		}	
	}
	
	public static void main(String[] args) throws IOException
	{
		/*File f=new File("temp");
		if(f.exists())
			f.delete();
		DataOutputStream out=new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
		LongDoubleNullNeighborhood nb=new LongDoubleNullNeighborhood();
		nb.setVertexValue(1.0);
		ArrayList<Long> edges=new ArrayList<Long>(10);
		for(int i=0; i<10; i++)
			edges.add((long)i);
		nb.setSimpleEdges(edges);
		nb.write(out);
		out.close();
		
		DataInputStream in = new DataInputStream (new BufferedInputStream(new FileInputStream(f)));
		LongDoubleNullNeighborhood newnb=new LongDoubleNullNeighborhood();
		newnb.readFields(in);
		in.close();
		System.out.println("nb: "+nb);
		System.out.println("newnb: "+newnb);*/
		
		File dir=new File("out.pr");
		File[] files=dir.listFiles();
		TreeMap<String, File> ordered=new TreeMap<String, File>();
		for(File file: files)
		{
			if(file.getName().contains("part"))
				ordered.put(file.getName(), file);
		}
		LongDoubleNullNeighborhood value=new LongDoubleNullNeighborhood();
		LongWritable key=new LongWritable();
		for(File file: ordered.values())
		{
			DataInputStream in = new DataInputStream (new BufferedInputStream(new FileInputStream(file)));
			while(true)
			{
				try{
					key.readFields(in);
					value.readFields(in);
					System.out.println("key: "+key+", value: "+value);
				}catch (EOFException e)
				{
					break;
				}catch (IOException e)
				{
					throw e;
				}
			}
			
			in.close();
		}
	}
}
