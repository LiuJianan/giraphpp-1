package org.apache.giraph.utils;

import org.apache.hadoop.io.LongWritable;

import com.google.common.collect.UnmodifiableIterator;

/**
 *  this class is added 
 */
public class UnmodifiableLongArrayIterator
        extends UnmodifiableIterator<LongWritable> {

    private final long[] arr;
    private int offset;

    public UnmodifiableLongArrayIterator(long[] arr) {
        this.arr = arr;
        offset = 0;
    }

    @Override
    public boolean hasNext() {
        return offset < arr.length;
    }

    @Override
    public LongWritable next() {
        return new LongWritable(arr[offset++]);
    }
}