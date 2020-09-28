/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SparseStore implements Store {

    private final NavigableMap<Integer, Double> bins;

    public SparseStore() {
        this.bins = new TreeMap<>();
    }

    private SparseStore(SparseStore store) {
        this.bins = new TreeMap<>(store.bins);
    }

    @Override
    public void add(int index) {
        bins.merge(index, 1.0, Double::sum);
    }

    @Override
    public void add(int index, double count) {
        if (count < 0) {
            throw new IllegalArgumentException("The count cannot be negative.");
        }
        if (count == 0) {
            return;
        }
        bins.merge(index, count, Double::sum);
    }

    @Override
    public void add(Bin bin) {
        if (bin.getCount() == 0) {
            return;
        }
        bins.merge(bin.getIndex(), bin.getCount(), Double::sum);
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
    public Iterator<Bin> getAscendingIterator() {
        return getBinIterator(bins);
    }

    @Override
    public Iterator<Bin> getDescendingIterator() {
        return getBinIterator(bins.descendingMap());
    }

    private static Iterator<Bin> getBinIterator(Map<Integer, Double> bins) {

        final Iterator<Entry<Integer, Double>> iterator = bins.entrySet().iterator();

        return new Iterator<Bin>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Bin next() {
                final Entry<Integer, Double> nextEntry = iterator.next();
                return new Bin(nextEntry.getKey(), nextEntry.getValue());
            }
        };
    }
}
