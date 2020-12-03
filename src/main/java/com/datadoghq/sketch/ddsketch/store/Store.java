/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An object that maps integers to counters. It can be seen as a collection of {@link Bin}, which are pairs of
 * indices and counters.
 */
public interface Store {

    /**
     * Increments the counter at the specified index.
     *
     * @param index the index of the counter to be incremented
     */
    default void add(int index) {
        add(index, 1);
    }

    /**
     * Updates the counter at the specified index.
     *
     * @param index the index of the counter to be updated
     * @param count a non-negative integer value
     * @throws IllegalArgumentException if {@code count} is negative
     */
    default void add(int index, long count) {
        add(index, (double) count);
    }

    /**
     * Updates the counter at the specified index.
     *
     * @param index the index of the counter to be updated
     * @param count a non-negative value
     * @throws IllegalArgumentException if {@code count} is negative
     */
    void add(int index, double count);

    /**
     * Updates the counter at the specified index.
     *
     * @param bin the bin to be used for updating the counter
     */
    default void add(Bin bin) {
        add(bin.getIndex(), bin.getCount());
    }

    /**
     * Merges another store into this one. This should be equivalent as running the {@code add} operations that have
     * been run on the other {@code store} on this one.
     *
     * @param store the store to be merged into this one
     */
    default void mergeWith(Store store) {
        store.getStream().forEach(this::add);
    }

    /**
     * @return a (deep) copy of this store
     */
    Store copy();


    /**
     * Zeros all counts in the store. The store behaves as if
     * empty after this call, but no underlying storage is released.
     */
    void clear();

    /**
     * @return {@code true} iff the {@code Store} does not contain any non-zero counter
     */
    default boolean isEmpty() {
        return getStream()
            .mapToDouble(Bin::getCount)
            .allMatch(count -> count == 0);
    }

    /**
     * @return the sum of the counters of this store
     */
    default double getTotalCount() {
        return getStream()
            .mapToDouble(Bin::getCount)
            .sum();
    }

    /**
     * @return the index of the lowest non-zero counter
     * @throws java.util.NoSuchElementException if the store is empty
     */
    default int getMinIndex() {
        return getAscendingStream()
            .filter(bin -> bin.getCount() > 0)
            .findFirst()
            .orElseThrow(NoSuchElementException::new)
            .getIndex();
    }

    /**
     * @return the index of the highest non-zero counter
     * @throws java.util.NoSuchElementException if the store is empty
     */
    default int getMaxIndex() {
        return getDescendingStream()
            .filter(bin -> bin.getCount() > 0)
            .findFirst()
            .orElseThrow(NoSuchElementException::new)
            .getIndex();
    }

    /**
     * @return a stream with the non-empty bins of this store as its source
     */
    default Stream<Bin> getStream() {
        return getAscendingStream();
    }

    /**
     * @return an ordered stream (from lowest to highest index) with the non-empty bins of this store as its source
     */
    default Stream<Bin> getAscendingStream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(getAscendingIterator(), 0), false);
    }

    /**
     * @return an ordered stream (from highest to lowest index) with the non-empty bins of this store as its source
     */
    default Stream<Bin> getDescendingStream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(getDescendingIterator(), 0), false);
    }

    /**
     * @return an iterator that iterates over the non-empty bins of this store, from lowest to highest index
     */
    // Needed because of JDK-8194952
    Iterator<Bin> getAscendingIterator();

    /**
     * @return an iterator that iterates over the non-empty bins of this store, from highest to lowest index
     */
    // Needed because of JDK-8194952
    Iterator<Bin> getDescendingIterator();

    /**
     * @return to what extent an inserted index can be shifted
     */
    default int maxShift() {
        return 0;
    }

    /**
     * Generates a protobuf representation of this {@code Store}.
     *
     * @return a protobuf representation of this {@code Store}
     */
    default com.datadoghq.sketch.ddsketch.proto.Store toProto() {
        final com.datadoghq.sketch.ddsketch.proto.Store.Builder storeBuilder =
            com.datadoghq.sketch.ddsketch.proto.Store.newBuilder();
        // In the general case, we use the sparse representation to encode bin counts.
        getStream().forEach(bin -> storeBuilder.putBinCounts(bin.getIndex(), bin.getCount()));
        return storeBuilder.build();
    }

    /**
     * Builds a new instance of {@code Store} based on the provided protobuf representation.
     *
     * @param storeSupplier the constructor of the {@link Store} of type {@code S} implementation
     * @param proto         the protobuf representation of a {@code Store}
     * @param <S>           the type of the {@code Store} to build
     * @return an instance of {@code Store} of type {@code S} that matches the protobuf representation
     */
    static <S extends Store> S fromProto(
        Supplier<? extends S> storeSupplier,
        com.datadoghq.sketch.ddsketch.proto.Store proto
    ) {
        final S store = storeSupplier.get();
        proto.getBinCountsMap().forEach(store::add);
        int index = proto.getContiguousBinIndexOffset();
        for (final double count : proto.getContiguousBinCountsList()) {
            store.add(index++, count);
        }
        return store;
    }
}
