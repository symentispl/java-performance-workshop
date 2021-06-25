package pl.symentis.mapreduce.core;

import java.util.Iterator;

public final class IteratorInput<E> implements Input<E> {

    private final Iterator<E> iterator;

    public IteratorInput(Iterator<E> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public E next() {
        return iterator.next();
    }

}