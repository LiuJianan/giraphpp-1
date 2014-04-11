package com.ibm.giraph.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapRedudeUtils {

	public static void deleteFileIfExistOnHDFS(Path outpath, Configuration conf) throws IOException {
		if (FileSystem.get(conf).exists(outpath)) {
			FileSystem.get(conf).delete(outpath, true);
		}
	}
	
	public static String getMessageString (Iterable msgs)
	{
		String str="";
		Iterator iter=msgs.iterator();
		while(iter.hasNext())
			str+=iter.next()+", ";
		return str;
	}
}
