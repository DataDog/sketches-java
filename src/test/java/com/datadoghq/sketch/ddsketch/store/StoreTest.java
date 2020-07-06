/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

abstract class StoreTest {

    abstract Store newStore();

    abstract Map<Integer, Long> getCounts(Bin[] bins);

    private static Map<Integer, Long> getCounts(Stream<Bin> bins) {
        return bins.collect(Collectors.groupingBy(Bin::getIndex, Collectors.summingLong(Bin::getCount)));
    }

    private static Map<Integer, Long> getCounts(Iterator<Bin> bins) {
        return getCounts(StreamSupport.stream(Spliterators.spliteratorUnknownSize(bins, 0), false));
    }

    private static Map<Integer, Long> getNonZeroCounts(Map<Integer, Long> counts) {
        return counts.entrySet().stream()
            .filter(entry -> entry.getValue() > 0)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private static void assertSameCounts(Map<Integer, Long> expected, Map<Integer, Long> actual) {
        assertEquals(getNonZeroCounts(expected), getNonZeroCounts(actual));
    }

    private void assertEncodes(Bin[] bins, Store store) {
        assertEncodes(getCounts(bins), store);
    }

    private static void assertEncodes(Map<Integer, Long> expectedCounts, Store store) {
        final long expectedTotalCount = expectedCounts.values().stream().mapToLong(count -> count).sum();
        assertEquals(expectedTotalCount, store.getTotalCount());
        if (expectedTotalCount == 0) {
            assertTrue(store.isEmpty());
            assertThrows(NoSuchElementException.class, store::getMinIndex);
            assertThrows(NoSuchElementException.class, store::getMaxIndex);
        } else {
            assertFalse(store.isEmpty());
            final int expectedMinIndex = expectedCounts.entrySet().stream()
                    .filter(entry -> entry.getValue() != 0)
                    .mapToInt(Entry::getKey)
                    .min()
                    .getAsInt();
            assertEquals(expectedMinIndex, store.getMinIndex());
            final int expectedMaxIndex = expectedCounts.entrySet().stream()
                    .filter(entry -> entry.getValue() != 0)
                    .mapToInt(Entry::getKey)
                    .max()
                    .getAsInt();
            assertEquals(expectedMaxIndex, store.getMaxIndex());
        }
        assertSameCounts(expectedCounts, getCounts(store.getStream()));
        assertSameCounts(expectedCounts, getCounts(store.getAscendingStream()));
        assertSameCounts(expectedCounts, getCounts(store.getDescendingStream()));
        assertSameCounts(expectedCounts, getCounts(store.getAscendingIterator()));
        assertSameCounts(expectedCounts, getCounts(store.getDescendingIterator()));
    }

    private static Bin[] toBins(int... values) {
        return Arrays.stream(values).mapToObj(value -> new Bin(value, 1)).toArray(Bin[]::new);
    }

    void testAdding(int... values) {
        final Store store = newStore();
        Arrays.stream(values).forEach(store::add);
        assertEncodes(toBins(values), store);

        testAdding(toBins(values));
    }

    void testAdding(Bin... bins) {
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(store::add);
            assertEncodes(bins, store);
        }
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(bin -> store.add(bin.getIndex(), bin.getCount()));
            assertEncodes(bins, store);
        }
    }

    void testMerging(int[]... values) {
        final Store store = newStore();
        Arrays.stream(values).forEach(storeValues -> {
            final Store intermediateStore = newStore();
            Arrays.stream(storeValues).forEach(intermediateStore::add);
            store.mergeWith(intermediateStore);
        });
        assertEncodes(
            Arrays.stream(values)
                .map(Arrays::stream)
                .flatMap(IntStream::boxed)
                .map(value -> new Bin(value, 1))
                .toArray(Bin[]::new),
            store
        );

        testMerging(
            Arrays.stream(values)
                .map(Arrays::stream)
                .map(storeValues -> storeValues.mapToObj(value -> new Bin(value, 1)).toArray(Bin[]::new))
                .toArray(Bin[][]::new)
        );
    }

    void testMerging(Bin[]... bins) {
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(storeBins -> {
                final Store intermediateStore = newStore();
                Arrays.stream(storeBins).forEach(intermediateStore::add);
                store.mergeWith(intermediateStore);
            });
            assertEncodes(Arrays.stream(bins).flatMap(Arrays::stream).toArray(Bin[]::new), store);
        }
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(storeBins -> {
                final Store intermediateStore = newStore();
                Arrays.stream(storeBins).forEach(bin -> intermediateStore.add(bin.getIndex(), bin.getCount()));
                store.mergeWith(intermediateStore);
            });
            assertEncodes(Arrays.stream(bins).flatMap(Arrays::stream).toArray(Bin[]::new), store);
        }
    }

    @Test
    void testEmpty() {
        testAdding(new int[]{});
    }

    @Test
    void testConstant() {
        testAdding(IntStream.range(0, 10000).map(i -> 0).toArray());
    }

    @Test
    void testIncreasingLinearly() {
        testAdding(IntStream.range(0, 10000).toArray());
    }

    @Test
    void testDecreasingLinearly() {
        testAdding(IntStream.range(0, 10000).map(i -> -i).toArray());
    }

    @Test
    void testIncreasingExponentially() {
        testAdding(IntStream.range(0, 16).map(i -> (int) Math.pow(2, i)).toArray());
    }

    @Test
    void testDecreasingExponentially() {
        testAdding(IntStream.range(0, 16).map(i -> -(int) Math.pow(2, i)).toArray());
    }

    @Test
    void testBinCounts() {
        testAdding(IntStream.range(0, 10).mapToObj(i -> new Bin(i, 2 * i)).toArray(Bin[]::new));
        testAdding(IntStream.range(0, 10).mapToObj(i -> new Bin(-i, 2 * i)).toArray(Bin[]::new));
    }

    @Test
    void testMergingEmpty() {
        testMerging(new int[]{}, new int[]{});
        testMerging(new int[]{}, new int[]{ 0 });
        testMerging(new int[]{ 0 }, new int[]{});
    }

    @Test
    void testMergingFarApart() {
        testMerging(new int[]{ -10000 }, new int[]{ 10000 });
        testMerging(new int[]{ 10000 }, new int[]{ -10000 });
        testMerging(new int[]{ 10000 }, new int[]{ -10000 }, new int[]{ 0 });
        testMerging(new int[]{ 10000, 0 }, new int[]{ -10000 }, new int[]{ 0 });
    }

    @Test
    void testMergingConstant() {
        testMerging(new int[]{ 2, 2 }, new int[]{ 2, 2, 2 }, new int[]{ 2 });
        testMerging(new int[]{ -8, -8 }, new int[]{}, new int[]{ -8 });
    }

    @Test
    void testCopyingEmpty() {
        newStore().copy();
    }

    @Test
    void testCopyingNonEmpty() {
        final Bin[] bins = new Bin[]{ new Bin(0, 1) };
        final Store store = newStore();
        Arrays.stream(bins).forEach(store::add);
        final Store copy = store.copy();
        assertEncodes(bins, copy);
    }
}
