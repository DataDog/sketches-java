package com.datadoghq.sketch.ddsketch.store;

import java.util.Iterator;

public interface Store {

    int getMinIndex();

    int getMaxIndex();

    void add(int index, long count);

    default void add(int index) {
        add(index, 1L);
    }

    default void mergeWith(Store store) {
        addAll(store.getAscendingBinIterator());
    }

    default void addAll(Iterator<Bin> binIterator) {
        while (binIterator.hasNext()) {
            final Bin bin = binIterator.next();
            add(bin.getIndex(), bin.getCount());
        }
    }

    Store copy();

    default long getTotalCount() {

        final Iterator<Bin> binIterator = getAscendingBinIterator();
        long count = 0L;
        while (binIterator.hasNext()) {
            count += binIterator.next().getCount();
        }
        return count;
    }

    default boolean isEmpty() {
        return getTotalCount() == 0;
    }

    Iterator<Bin> getAscendingBinIterator();

    Iterator<Bin> getDescendingBinIterator();
}
