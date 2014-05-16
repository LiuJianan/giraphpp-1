package com.ibm.giraph.utils;

import java.util.Iterator;


public class UnmodifiableSingleItem<T> implements Iterable<T>{

	T item=null;
	public UnmodifiableSingleItem(T item)
	{
		this.item=item;
	}
	@Override
	public Iterator<T> iterator() {
		return new UnmodifiableSingleItemIterator<T>(item);
	}
}
