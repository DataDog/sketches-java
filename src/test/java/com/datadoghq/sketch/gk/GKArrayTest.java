/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.gk;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

abstract class GKArrayTest {

    @Test
    protected void throwsExceptionWhenExpected() {

        final GKArray<Double> emptySketch = newSketch();

        emptySketchAssertions(emptySketch);

        final GKArray<Double> nonEmptySketch = newSketch();
        nonEmptySketch.accept(0D);
        nonEmptySketchAssertions(nonEmptySketch);

    }

    private void emptySketchAssertions(GKArray<Double> emptySketch) {
        assertThrows(
            NoSuchElementException.class,
            emptySketch::getMinValue
        );

        assertThrows(
            NoSuchElementException.class,
            emptySketch::getMaxValue
        );

        assertThrows(
            NoSuchElementException.class,
            () -> emptySketch.getValueAtQuantile(0.5)
        );

        assertThrows(
            NullPointerException.class,
            () -> emptySketch.mergeWith(null)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> emptySketch.accept(0D, -1)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> emptySketch.accept(1D, -1)
        );
    }

    private void nonEmptySketchAssertions(GKArray<Double> nonEmptySketch) {
        assertThrows(
            IllegalArgumentException.class,
            () -> nonEmptySketch.getValueAtQuantile(-0.1)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> nonEmptySketch.getValueAtQuantile(1.1)
        );
    }

    @Test
    protected void clearSketchShouldBehaveEmpty() {
        final GKArray<Double> sketch = newSketch();
        sketch.accept(0D);
        sketch.clear();
        assertTrue(sketch.isEmpty());
        emptySketchAssertions(sketch);
    }

    protected void test(boolean merged, double[] values, GKArray<Double> sketch) {
        assertEncodes(merged, values, sketch);
    }

    protected final void assertEncodes(boolean merged, double[] values, GKArray<Double> sketch) {
        assertEquals(values.length, sketch.getCount());
        if (values.length == 0) {
            assertTrue(sketch.isEmpty());
        } else {
            assertFalse(sketch.isEmpty());
            final double[] sortedValues = Arrays.copyOf(values, values.length);
            Arrays.sort(sortedValues);
            assertAccurate(merged, sortedValues, 0, sketch.getMinValue());
            assertAccurate(merged, sortedValues, 1, sketch.getMaxValue());
            for (double quantile = 0; quantile <= 1; quantile += 0.01) {
                assertAccurate(merged, sortedValues, quantile, sketch.getValueAtQuantile(quantile));
            }
        }
    }

    protected void testAdding(double... values) {
        {
            final GKArray<Double> sketch = newSketch();
            Arrays.stream(values).forEach(sketch::accept);
            test(false, values, sketch);
        }
        {
            final GKArray<Double> sketch = newSketch();
            Arrays.stream(values).boxed()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .forEach(sketch::accept);
            test(false, values, sketch);
        }
    }

    protected void testMerging(double[]... values) {
        {
            final GKArray<Double> sketch = newSketch();
            Arrays.stream(values).forEach(sketchValues -> {
                final GKArray<Double> intermediateSketch = newSketch();
                Arrays.stream(sketchValues).forEach(intermediateSketch::accept);
                sketch.mergeWith(intermediateSketch);
            });
            test(true, Arrays.stream(values).flatMapToDouble(Arrays::stream).toArray(), sketch);
        }
        {
            final GKArray<Double> sketch = newSketch();
            Arrays.stream(values).forEach(sketchValues -> {
                final GKArray<Double> intermediateSketch = newSketch();
                Arrays.stream(sketchValues).boxed()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .forEach(intermediateSketch::accept);
                sketch.mergeWith(intermediateSketch);
            });
            test(true, Arrays.stream(values).flatMapToDouble(Arrays::stream).toArray(), sketch);
        }
    }

    @Test
    void testEmpty() {
        testAdding();
    }

    @Test
    void testConstant() {
        testAdding(0);
        testAdding(1);
        testAdding(1, 1, 1);
        testAdding(10, 10, 10);
        testAdding(IntStream.range(0, 10000).mapToDouble(i -> 2).toArray());
        testAdding(10, 10, 11, 11, 11);
    }

    @Test
    void testSingleValue() {

        testAdding(0);

        testAdding(10);
    }

    @Test
    void testWithZeros() {

        testAdding(IntStream.range(0, 100).mapToDouble(i -> 0).toArray());

        testAdding(DoubleStream.concat(
            IntStream.range(0, 10).mapToDouble(i -> 0),
            IntStream.range(0, 100).mapToDouble(i -> i)
        ).toArray());

        testAdding(DoubleStream.concat(
            IntStream.range(0, 100).mapToDouble(i -> i),
            IntStream.range(0, 10).mapToDouble(i -> 0)
        ).toArray());
    }

    @Test
    void testWithoutZeros() {

        testAdding(IntStream.range(1, 100).mapToDouble(i -> i).toArray());
    }

    @Test
    void testIncreasingLinearly() {
        testAdding(IntStream.range(0, 10000).mapToDouble(v -> v).toArray());
    }

    @Test
    void testDecreasingLinearly() {
        testAdding(IntStream.range(0, 10000).mapToDouble(v -> 10000 - v).toArray());
    }

    @Test
    void testIncreasingExponentially() {
        testAdding(IntStream.range(0, 100).mapToDouble(Math::exp).toArray());
    }

    @Test
    void testDecreasingExponentially() {
        testAdding(IntStream.range(0, 100).mapToDouble(i -> Math.exp(-i)).toArray());
    }

    @Test
    void testMergingEmpty() {
        testMerging(new double[]{}, new double[]{});
        testMerging(new double[]{}, new double[]{ 0 });
        testMerging(new double[]{ 0 }, new double[]{});
        testMerging(new double[]{}, new double[]{ 2 });
        testMerging(new double[]{ 2 }, new double[]{});
    }

    @Test
    void testMergingConstant() {
        testMerging(new double[]{ 1, 1 }, new double[]{ 1, 1, 1 });
    }

    @Test
    void testMergingFarApart() {
        testMerging(new double[]{ 0 }, new double[]{ 10000 });
        testMerging(new double[]{ 10000 }, new double[]{ 20000 });
        testMerging(new double[]{ 20000 }, new double[]{ 10000 });
        testMerging(new double[]{ 10000 }, new double[]{ 0 }, new double[]{ 0 });
        testMerging(new double[]{ 10000, 0 }, new double[]{ 10000 }, new double[]{ 0 });
    }

    // Tests with random values

    private int randomSize(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }

    private double randomValue() {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Test
    @Disabled
        // to avoid making the tests non-deterministic
    void testAddingRandomly() {

        final int numTests = 1000;
        final int maxNumValues = 10000;

        for (int i = 0; i < numTests; i++) {

            final double[] values = IntStream.range(0, randomSize(maxNumValues))
                .mapToDouble(j -> randomValue())
                .toArray();

            testAdding(values);
        }
    }

    @Test
    @Disabled
        // to avoid making the tests non-deterministic
    void testMergingRandomly() {

        final int numTests = 1000;
        final int maxNumSketches = 100;
        final int maxNumValuesPerSketch = 1000;

        for (int i = 0; i < numTests; i++) {

            final double[][] values = IntStream.range(0, randomSize(maxNumSketches))
                .mapToObj(j -> IntStream.range(0, randomSize(maxNumValuesPerSketch))
                    .mapToDouble(k -> randomValue())
                    .toArray()
                ).toArray(length -> new double[length][0]);

            testMerging(values);
        }
    }

    @Test
    void testCopyingEmpty() {
        newSketch().copy();
    }

    @Test
    void testCopyingNonEmpty() {
        final double[] values = new double[]{ 0 };
        final GKArray<Double> sketch = newSketch();
        Arrays.stream(values).forEach(sketch::accept);
        final GKArray<Double> copy = sketch.copy();
        test(false, values, copy);
    }

    abstract double rankAccuracy();

    public GKArray<Double> newSketch() {
        return new GKArray<>(rankAccuracy());
    }

    protected void assertAccurate(boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {

        final double rankAccuracy = (merged ? 2 : 1) * rankAccuracy();

        // Check the rank accuracy.

        final int minExpectedRank = (int) Math.floor(Math.max(quantile - rankAccuracy, 0) * sortedValues.length);
        final int maxExpectedRank = (int) Math.ceil(Math.min(quantile + rankAccuracy, 1) * sortedValues.length);

        int searchIndex = Arrays.binarySearch(sortedValues, actualQuantileValue);
        if (searchIndex < 0) {
            searchIndex = -searchIndex - 1;
        }
        int index = searchIndex;
        while (index > 0 && sortedValues[index - 1] >= actualQuantileValue) {
            index--;
        }
        int minRank = index;
        while (index < sortedValues.length && sortedValues[index] <= actualQuantileValue) {
            index++;
        }
        int maxRank = index;

        if (maxRank < minExpectedRank || minRank > maxExpectedRank) {
            fail();
        }
    }

    static class GKArrayTest1 extends GKArrayTest {

        @Override
        double rankAccuracy() {
            return 1e-1;
        }
    }

    static class GKArrayTest2 extends GKArrayTest {

        @Override
        double rankAccuracy() {
            return 1e-2;
        }
    }

    static class GKArrayTest3 extends GKArrayTest {

        @Override
        double rankAccuracy() {
            return 1e-3;
        }
    }
}
