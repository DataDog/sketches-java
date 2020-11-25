/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

abstract class StoreTest {

    abstract Store newStore();

    abstract Map<Integer, Double> getCounts(Bin[] bins);

    private static Map<Integer, Double> getCounts(Stream<Bin> bins) {
        return bins.collect(Collectors.groupingBy(Bin::getIndex, Collectors.summingDouble(Bin::getCount)));
    }

    private static Map<Integer, Double> getCounts(Iterator<Bin> bins) {
        return getCounts(StreamSupport.stream(Spliterators.spliteratorUnknownSize(bins, 0), false));
    }

    private static Map<Integer, Double> getNonZeroCounts(Map<Integer, Double> counts) {
        return counts.entrySet().stream()
            .filter(entry -> entry.getValue() > 0)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private static void assertSameCounts(Map<Integer, Double> expected, Map<Integer, Double> actual) {
        assertEquals(new HashSet<>(expected.keySet()), new HashSet<>(actual.keySet()));
        for (final int key : expected.keySet()) {
            assertEquals(expected.get(key), actual.get(key), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        }
    }

    private void test(Bin[] bins, Store store) {
        final Map<Integer, Double> expectedNonZeroCounts = getNonZeroCounts(getCounts(bins));
        assertEncodes(expectedNonZeroCounts, store);
        // Test protobuf round-trip
        assertEncodes(expectedNonZeroCounts, Store.fromProto(this::newStore, store.toProto()));
    }

    private static void assertEncodes(Map<Integer, Double> expectedCounts, Store store) {
        final double expectedTotalCount = expectedCounts.values().stream().mapToDouble(count -> count).sum();
        assertEquals(expectedTotalCount, store.getTotalCount(), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
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
        test(toBins(values), store);

        testAdding(toBins(values));
    }

    void testAdding(Bin... bins) {
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(store::add);
            test(bins, store);
        }
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(bin -> store.add(bin.getIndex(), bin.getCount()));
            test(bins, store);
        }
    }

    void testMerging(int[]... values) {
        final Store store = newStore();
        Arrays.stream(values).forEach(storeValues -> {
            final Store intermediateStore = newStore();
            Arrays.stream(storeValues).forEach(intermediateStore::add);
            store.mergeWith(intermediateStore);
        });
        test(
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
            test(Arrays.stream(bins).flatMap(Arrays::stream).toArray(Bin[]::new), store);
        }
        {
            final Store store = newStore();
            Arrays.stream(bins).forEach(storeBins -> {
                final Store intermediateStore = newStore();
                Arrays.stream(storeBins).forEach(bin -> intermediateStore.add(bin.getIndex(), bin.getCount()));
                store.mergeWith(intermediateStore);
            });
            test(Arrays.stream(bins).flatMap(Arrays::stream).toArray(Bin[]::new), store);
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
    void testClear() {
        final Store store = newStore();
        assertTrue(store.isEmpty());
        store.clear();
        assertTrue(store.isEmpty());
        int[] beforeClear = IntStream.range(0, 10000).toArray();
        Arrays.stream(beforeClear).forEach(store::add);
        test(toBins(beforeClear), store);
        assertFalse(store.isEmpty());
        store.clear();
        assertTrue(store.isEmpty());
        // add some more values at an offset
        int[] afterClear = IntStream.range(10000, 20000).toArray();
        Arrays.stream(afterClear).forEach(store::add);
        test(toBins(afterClear), store);
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
    void testNonIntegerCounts() {
        testAdding(IntStream.range(0, 10).mapToObj(i -> new Bin(i, Math.log(i + 1))).toArray(Bin[]::new));
        testAdding(IntStream.range(0, 10).mapToObj(i -> new Bin(-i, Math.log(i + 1))).toArray(Bin[]::new));
    }

    @Test
    void testExtremeValues() {
        testAdding(Integer.MIN_VALUE);
        testAdding(Integer.MAX_VALUE);
        testAdding(0, Integer.MIN_VALUE);
        testAdding(0, Integer.MAX_VALUE);
        testAdding(Integer.MIN_VALUE, Integer.MAX_VALUE);
        testAdding(Integer.MAX_VALUE, Integer.MIN_VALUE);
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
        testMerging(new int[]{ -10000, 10000 }, new int[]{ -5000, 5000 });
        testMerging(new int[]{ -5000, 5000 }, new int[]{ -10000, 10000 });
        testMerging(new int[]{ -5000, 10000 }, new int[]{ -10000, 5000 });
        testMerging(new int[]{ 10000, 0 }, new int[]{ -10000 }, new int[]{ 0 });
    }

    @Test
    void testMergingConstant() {
        testMerging(new int[]{ 2, 2 }, new int[]{ 2, 2, 2 }, new int[]{ 2 });
        testMerging(new int[]{ -8, -8 }, new int[]{}, new int[]{ -8 });
    }

    @Test
    void testMergingNonIntegerCounts() {
        testMerging(
            new Bin[]{ new Bin(3, Math.PI) },
            new Bin[]{ new Bin(3, Math.E) }
        );
        testMerging(
            new Bin[]{ new Bin(0, 0.1), new Bin(2, 0.3) },
            new Bin[]{ new Bin(-1, 0.9), new Bin(0, 0.7), new Bin(2, 0.1) }
        );
    }

    @Test
    void testMergingExtremeValues() {
        testMerging(new int[]{ 0 }, new int[]{ Integer.MIN_VALUE });
        testMerging(new int[]{ 0 }, new int[]{ Integer.MAX_VALUE });
        testMerging(new int[]{ Integer.MIN_VALUE }, new int[]{ 0 });
        testMerging(new int[]{ Integer.MAX_VALUE }, new int[]{ 0 });
        testMerging(new int[]{ Integer.MIN_VALUE }, new int[]{ Integer.MIN_VALUE });
        testMerging(new int[]{ Integer.MAX_VALUE }, new int[]{ Integer.MAX_VALUE });
        testMerging(new int[]{ Integer.MIN_VALUE }, new int[]{ Integer.MAX_VALUE });
        testMerging(new int[]{ Integer.MAX_VALUE }, new int[]{ Integer.MIN_VALUE });
        testMerging(new int[]{ 0 }, new int[]{ Integer.MIN_VALUE, Integer.MAX_VALUE });
        testMerging(new int[]{ Integer.MIN_VALUE, Integer.MAX_VALUE }, new int[]{ 0 });
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
        test(bins, copy);
    }

    @Test
    public void testCountsPreservedAfterCopy() {
        Store store = newStore();
        store.add(10);
        store.add(100);
        Store copy = store.copy();
        assertEquals(store.getTotalCount(), copy.getTotalCount(), 1e-7);
    }
}
