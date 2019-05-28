package com.datadoghq.sketch.ddsketch.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

abstract class StoreTest {

    abstract Store newStore();

    abstract Map<Integer, Long> getCounts(Collection<Integer> values);

    private Map<Integer, Long> getCounts(int[] values) {
        return getCounts(Arrays.stream(values).boxed().collect(Collectors.toList()));
    }

    private Map<Integer, Long> getCounts(int[][] values) {
        return getCounts(Arrays.stream(values)
                .flatMap(v -> Arrays.stream(v).boxed())
                .collect(Collectors.toList()));
    }

    private void testAdding(int... values) {

        final Store store = newStore();

        for (final int value : values) {
            store.add(value);
        }

        assertEquals(getCounts(values), getCounts(store));
    }

    private void testMerging(int[]... values) {

        final Store store = newStore();

        for (final int[] storeValues : values) {
            final Store intermediateStore = newStore();
            for (final int value : storeValues) {
                store.add(value);
            }
            store.mergeWith(intermediateStore);
        }

        assertEquals(getCounts(values), getCounts(store));
    }

    private static Map<Integer, Long> getCounts(Store store) {

        final Map<Integer, Long> counts = new HashMap<>();
        final Iterator<Bin> iterator = store.getAscendingBinIterator();
        while (iterator.hasNext()) {
            final Bin bin = iterator.next();
            if (bin.getCount() != 0) {
                counts.put(bin.getIndex(), bin.getCount());
            }
        }
        return counts;
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
    void testMergingEmpty() {

        testMerging(new int[]{}, new int[]{ 0 });
        testMerging(new int[]{ 0 }, new int[]{});
    }

    @Test
    void testMergingFarApart() {

        testMerging(new int[]{ -10000 }, new int[]{ 10000 });
        testMerging(new int[]{ 10000 }, new int[]{ -10000 });
    }

    @Test
    void testAddingRandomly() {

        final int numTests = 1000;
        final int maxNumValues = 10000;

        for (int i = 0; i < numTests; i++) {

            final int[] values = IntStream.range(0, randomSize(maxNumValues))
                    .map(j -> randomValue())
                    .toArray();

            testAdding(values);
        }
    }

    @Test
    void testMergingRandomly() {

        final int numTests = 1000;
        final int maxNumSketches = 100;
        final int maxNumValuesPerSketch = 1000;

        for (int i = 0; i < numTests; i++) {

            final int[][] values = IntStream.range(0, randomSize(maxNumSketches))
                    .mapToObj(j -> IntStream.range(0, randomSize(maxNumValuesPerSketch))
                            .map(k -> randomValue())
                            .toArray()
                    ).toArray(length -> new int[length][0]);

            testMerging(values);
        }
    }

    private int randomValue() {
        return ThreadLocalRandom.current().nextInt(-100, 100);
    }

    private int randomSize(int maxSize) {
        return ThreadLocalRandom.current().nextInt(0, maxSize + 1);
    }
}
