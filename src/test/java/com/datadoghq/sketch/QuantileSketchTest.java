/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public abstract class QuantileSketchTest<QS extends QuantileSketch<QS>> {

  protected static final double EPSILON = AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR;
  private static final Offset<Double> DOUBLE_OFFSET = offset(EPSILON);

  protected abstract QS newSketch();

  protected abstract void assertQuantileAccurate(
      boolean merged, double[] sortedValues, double quantile, double actualQuantileValue);

  protected abstract void assertMinAccurate(double[] sortedValues, double actualMinValue);

  protected abstract void assertMaxAccurate(double[] sortedValues, double actualMaxValue);

  protected abstract void assertSumAccurate(double[] sortedValues, double actualSumValue);

  protected abstract void assertAverageAccurate(double[] sortedValues, double actualAverageValue);

  @Test
  protected void throwsExceptionWhenExpected() {

    final QS emptySketch = newSketch();

    emptySketchAssertions(emptySketch);

    final QS nonEmptySketch = newSketch();
    nonEmptySketch.accept(0);
    nonEmptySketchAssertions(nonEmptySketch);
  }

  private void emptySketchAssertions(QS emptySketch) {
    assertEquals(0, emptySketch.getSum());

    assertEquals(0, emptySketch.getCount());

    assertThrows(NoSuchElementException.class, emptySketch::getAverage);

    assertThrows(NoSuchElementException.class, emptySketch::getMinValue);

    assertThrows(NoSuchElementException.class, emptySketch::getMaxValue);

    assertThrows(NoSuchElementException.class, () -> emptySketch.getValueAtQuantile(0.5));

    assertThrows(
        NoSuchElementException.class, () -> emptySketch.getValuesAtQuantiles(new double[] {0.5}));

    assertThrows(NullPointerException.class, () -> emptySketch.mergeWith(null));

    assertThrows(IllegalArgumentException.class, () -> emptySketch.accept(0, -1));

    assertThrows(IllegalArgumentException.class, () -> emptySketch.accept(1, -1));
  }

  private void nonEmptySketchAssertions(QS nonEmptySketch) {
    assertThrows(IllegalArgumentException.class, () -> nonEmptySketch.getValueAtQuantile(-0.1));

    assertThrows(IllegalArgumentException.class, () -> nonEmptySketch.getValueAtQuantile(1.1));

    assertThrows(
        IllegalArgumentException.class,
        () -> nonEmptySketch.getValuesAtQuantiles(new double[] {0, -0.1}));

    assertThrows(
        IllegalArgumentException.class,
        () -> nonEmptySketch.getValuesAtQuantiles(new double[] {1.1, 1}));
  }

  @Test
  protected void clearSketchShouldBehaveEmpty() {
    final QS sketch = newSketch();
    sketch.accept(0);
    sketch.clear();
    assertTrue(sketch.isEmpty());
    emptySketchAssertions(sketch);
  }

  protected void test(boolean merged, double[] values, QS sketch) {
    assertEncodes(merged, values, sketch);
  }

  protected final void assertEncodes(boolean merged, double[] values, QS sketch) {
    assertThat(sketch.getCount()).isCloseTo(values.length, DOUBLE_OFFSET);
    if (values.length == 0) {
      assertTrue(sketch.isEmpty());
    } else {
      assertFalse(sketch.isEmpty());
      final double[] sortedValues = Arrays.copyOf(values, values.length);
      Arrays.sort(sortedValues);

      final double minValue = sketch.getMinValue();
      final double maxValue = sketch.getMaxValue();

      assertMinAccurate(sortedValues, minValue);
      assertMaxAccurate(sortedValues, maxValue);

      for (double quantile = 0; quantile <= 1; quantile += 0.01) {
        final double valueAtQuantile = sketch.getValueAtQuantile(quantile);
        assertQuantileAccurate(merged, sortedValues, quantile, valueAtQuantile);

        // For consistency, we require any value to between the min and the max value that are
        // returned by the sketch.
        assertTrue(valueAtQuantile >= minValue);
        assertTrue(valueAtQuantile <= maxValue);

        final double[] valuesAtQuantiles = sketch.getValuesAtQuantiles(new double[] {quantile});
        assertEquals(valuesAtQuantiles.length, 1);
        assertEquals(valueAtQuantile, valuesAtQuantiles[0]);
      }

      assertSumAccurate(values, sketch.getSum());
      assertAverageAccurate(values, sketch.getAverage());
    }
  }

  protected void testAdding(double... values) {
    {
      final QS sketch = newSketch();
      Arrays.stream(values).forEach(sketch);
      test(false, values, sketch);
    }
    {
      final QS sketch = newSketch();
      Arrays.stream(values)
          .boxed()
          .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
          .forEach(sketch::accept);
      test(false, values, sketch);
    }
  }

  protected void testMerging(double[]... values) {
    {
      final QS sketch = newSketch();
      Arrays.stream(values)
          .forEach(
              sketchValues -> {
                final QS intermediateSketch = newSketch();
                Arrays.stream(sketchValues).forEach(intermediateSketch);
                sketch.mergeWith(intermediateSketch);
              });
      test(true, Arrays.stream(values).flatMapToDouble(Arrays::stream).toArray(), sketch);
    }
    {
      final QS sketch = newSketch();
      Arrays.stream(values)
          .forEach(
              sketchValues -> {
                final QS intermediateSketch = newSketch();
                Arrays.stream(sketchValues)
                    .boxed()
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

    testAdding(
        DoubleStream.concat(
                IntStream.range(0, 10).mapToDouble(i -> 0),
                IntStream.range(0, 100).mapToDouble(i -> i))
            .toArray());

    testAdding(
        DoubleStream.concat(
                IntStream.range(0, 100).mapToDouble(i -> i),
                IntStream.range(0, 10).mapToDouble(i -> 0))
            .toArray());
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
    testMerging(new double[] {}, new double[] {});
    testMerging(new double[] {}, new double[] {0});
    testMerging(new double[] {0}, new double[] {});
    testMerging(new double[] {}, new double[] {2});
    testMerging(new double[] {2}, new double[] {});
  }

  @Test
  void testMergingConstant() {
    testMerging(new double[] {1, 1}, new double[] {1, 1, 1});
  }

  @Test
  void testMergingFarApart() {
    testMerging(new double[] {0}, new double[] {10000});
    testMerging(new double[] {10000}, new double[] {20000});
    testMerging(new double[] {20000}, new double[] {10000});
    testMerging(new double[] {10000}, new double[] {0}, new double[] {0});
    testMerging(new double[] {10000, 0}, new double[] {10000}, new double[] {0});
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

      final double[] values =
          IntStream.range(0, randomSize(maxNumValues)).mapToDouble(j -> randomValue()).toArray();

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

      final double[][] values =
          IntStream.range(0, randomSize(maxNumSketches))
              .mapToObj(
                  j ->
                      IntStream.range(0, randomSize(maxNumValuesPerSketch))
                          .mapToDouble(k -> randomValue())
                          .toArray())
              .toArray(length -> new double[length][0]);

      testMerging(values);
    }
  }

  @Test
  void testCopyingEmpty() {
    newSketch().copy();
  }

  @Test
  void testCopyingNonEmpty() {
    final double[] values = new double[] {0};
    final QS sketch = newSketch();
    Arrays.stream(values).forEach(sketch);
    final QS copy = sketch.copy();
    test(false, values, copy);
  }
}
