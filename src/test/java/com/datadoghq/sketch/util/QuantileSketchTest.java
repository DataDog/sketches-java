package com.datadoghq.sketch.util;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public abstract class QuantileSketchTest<S> {

    public abstract S newSketch();

    public abstract void addToSketch(S sketch, double value);

    private void addToSketch(S sketch, double[] values) {
        Arrays.stream(values).forEach(value -> addToSketch(sketch, value));
    }

    public abstract void merge(S sketch, S other);

    public abstract void assertAddingAccurate(S sketch, double[] values);

    public abstract void assertMergingAccurate(S sketch, double[] values);

    private void testAdding(double... values) {

        final S sketch = newSketch();
        addToSketch(sketch, values);
        assertAddingAccurate(sketch, values);
    }

    private void testMerging(double[]... values) {

        final S sketch = newSketch();
        for (final double[] sketchValues : values) {
            final S s = newSketch();
            addToSketch(s, sketchValues);
            merge(sketch, s);
        }
        final double[] allValues = Arrays.stream(values)
                .flatMapToDouble(Arrays::stream)
                .toArray();
        assertMergingAccurate(sketch, allValues);
    }

    private int randomSize(int maxSize) {
        return ThreadLocalRandom.current().nextInt(0, maxSize + 1);
    }

    public double randomValue() {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Test
    void testEmpty() {

        testAdding();
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
}
