package com.ibm.giraph.utils;

import com.google.common.collect.UnmodifiableIterator;

public class UnmodifiableSingleItemIterator<E>
extends UnmodifiableIterator<E> {

	private E elem;
	private boolean hasnext;
	
	public UnmodifiableSingleItemIterator(E e) {
		elem=e;
		if(elem==null)
			hasnext=false;
		else
			hasnext = true;
	}
	
	@Override
	public boolean hasNext() {
	    return hasnext;
	}
	
	@Override
	public E next() {
		hasnext=false;
	    return elem;
	}
}
