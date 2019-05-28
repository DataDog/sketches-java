package com.datadoghq.sketch.ddsketch.store;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SparseStore implements Store {

    private final NavigableMap<Integer, Long> bins;

    public SparseStore() {
        this.bins = new TreeMap<>();
    }

    private SparseStore(SparseStore store) {
        this.bins = new TreeMap<>(store.bins);
    }

    @Override
    public void add(int index, long count) {
        bins.merge(index, count, Long::sum);
    }

    @Override
    public Store copy() {
        return new SparseStore(this);
    }

    @Override
    public int getMinIndex() {
        return bins.firstKey();
    }

    @Override
    public int getMaxIndex() {
        return bins.lastKey();
    }

    @Override
    public Iterator<Bin> getAscendingBinIterator() {
        return getBinIterator(bins);
    }

    @Override
    public Iterator<Bin> getDescendingBinIterator() {
        return getBinIterator(bins.descendingMap());
    }

    private static Iterator<Bin> getBinIterator(Map<Integer, Long> bins) {

        final Iterator<Entry<Integer, Long>> iterator = bins.entrySet().iterator();

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Bin next() {
                final Entry<Integer, Long> nextEntry = iterator.next();
                return new Bin(nextEntry.getKey(), nextEntry.getValue());
            }
        };
    }
}
