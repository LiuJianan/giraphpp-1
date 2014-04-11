package org.apache.giraph.utils;

import org.apache.hadoop.io.DoubleWritable;
import com.google.common.collect.UnmodifiableIterator;

/**
 *  this class is added 
 */
public class UnmodifiableDoubleArrayIterator
        extends UnmodifiableIterator<DoubleWritable> {

    private final double[] arr;
    private int offset;

    public UnmodifiableDoubleArrayIterator(double[] arr) {
        this.arr = arr;
        offset = 0;
    }

    @Override
    public boolean hasNext() {
        return offset < arr.length;
    }

    @Override
    public DoubleWritable next() {
        return new DoubleWritable(arr[offset++]);
    }
}