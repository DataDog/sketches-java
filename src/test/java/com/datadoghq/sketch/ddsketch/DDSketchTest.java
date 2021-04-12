/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

abstract class DDSketchTest extends QuantileSketchTest<DDSketch> {

  abstract double relativeAccuracy();

  IndexMapping mapping() {
    return new LogarithmicMapping(relativeAccuracy());
  }

  Supplier<Store> storeSupplier() {
    return UnboundedSizeDenseStore::new;
  }

  @Override
  public DDSketch newSketch() {
    return new DDSketch(mapping(), storeSupplier());
  }

  @Override
  protected void assertQuantileAccurate(
      boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {

    final double lowerQuantileValue =
        sortedValues[(int) Math.floor(quantile * (sortedValues.length - 1))];
    final double upperQuantileValue =
        sortedValues[(int) Math.ceil(quantile * (sortedValues.length - 1))];

    assertAccurate(lowerQuantileValue, upperQuantileValue, actualQuantileValue);
  }

  @Override
  protected void assertMinAccurate(double[] sortedValues, double actualMinValue) {
    assertAccurate(sortedValues[0], actualMinValue);
  }

  @Override
  protected void assertMaxAccurate(double[] sortedValues, double actualMaxValue) {
    assertAccurate(sortedValues[sortedValues.length - 1], actualMaxValue);
  }

  @Override
  protected void assertSumAccurate(double[] sortedValues, double actualSumValue) {
    // The sum is accurate if the values that have been added to the sketch have same sign.
    if (sortedValues[0] >= 0 || sortedValues[sortedValues.length - 1] <= 0) {
      assertAccurate(Arrays.stream(sortedValues).sum(), actualSumValue);
    }
  }

  @Override
  protected void assertAverageAccurate(double[] sortedValues, double actualAverageValue) {
    // The average is accurate if the values that have been added to the sketch have same sign.
    if (sortedValues[0] >= 0 || sortedValues[sortedValues.length - 1] <= 0) {
      assertAccurate(Arrays.stream(sortedValues).average().getAsDouble(), actualAverageValue);
    }
  }

  private void assertAccurate(double minExpected, double maxExpected, double actual) {
    final double relaxedMinExpected =
        minExpected > 0
            ? minExpected * (1 - relativeAccuracy())
            : minExpected * (1 + relativeAccuracy());
    final double relaxedMaxExpected =
        maxExpected > 0
            ? maxExpected * (1 + relativeAccuracy())
            : maxExpected * (1 - relativeAccuracy());

    if (actual < relaxedMinExpected - AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR
        || actual > relaxedMaxExpected + AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR) {
      fail();
    }
  }

  private void assertAccurate(double expected, double actual) {
    assertAccurate(expected, expected, actual);
  }

  @Test
  void testNegativeConstants() {
    testAdding(0);
    testAdding(-1);
    testAdding(-1, -1, -1);
    testAdding(-10, -10, -10);
    testAdding(IntStream.range(0, 10000).mapToDouble(i -> -2).toArray());
    testAdding(-10, -10, -11, -11, -11);
  }

  @Test
  void testNegativeAndPositiveConstants() {
    testAdding(0);
    testAdding(-1, 1);
    testAdding(-1, -1, -1, 1, 1, 1);
    testAdding(-10, -10, -10, 10, 10, 10);
    testAdding(IntStream.range(0, 20000).mapToDouble(i -> i % 2 == 0 ? 2 : -2).toArray());
    testAdding(-10, -10, -11, -11, -11, 10, 10, 11, 11, 11);
  }

  @Test
  void testWithZeros() {

    testAdding(IntStream.range(0, 100).mapToDouble(i -> 0).toArray());

    testAdding(
        DoubleStream.concat(
                IntStream.range(0, 10).mapToDouble(i -> 0),
                IntStream.range(-100, 100).mapToDouble(i -> i))
            .toArray());

    testAdding(
        DoubleStream.concat(
                IntStream.range(-100, 100).mapToDouble(i -> i),
                IntStream.range(0, 10).mapToDouble(i -> 0))
            .toArray());
  }

  @Test
  void testWithoutZeros() {
    testAdding(
        DoubleStream.concat(
                IntStream.range(-100, -1).mapToDouble(i -> i),
                IntStream.range(1, 100).mapToDouble(i -> i))
            .toArray());
  }

  @Test
  void testNegativeNumbersIncreasingLinearly() {
    testAdding(IntStream.range(-10000, 0).mapToDouble(v -> v).toArray());
  }

  @Test
  void testNegativeAndPositiveNumbersIncreasingLinearly() {
    testAdding(IntStream.range(-10000, 10000).mapToDouble(v -> v).toArray());
  }

  @Test
  void testNegativeNumbersDecreasingLinearly() {
    testAdding(IntStream.range(0, 10000).mapToDouble(v -> -v).toArray());
  }

  @Test
  void testNegativeAndPositiveNumbersDecreasingLinearly() {
    testAdding(IntStream.range(0, 20000).mapToDouble(v -> 10000 - v).toArray());
  }

  @Test
  void testNegativeNumbersIncreasingExponentially() {
    testAdding(IntStream.range(0, 100).mapToDouble(i -> -Math.exp(i)).toArray());
  }

  @Test
  void testNegativeAndPositiveNumbersIncreasingExponentially() {
    testAdding(
        DoubleStream.concat(
                IntStream.range(0, 100).mapToDouble(i -> -Math.exp(i)),
                IntStream.range(0, 100).mapToDouble(Math::exp))
            .toArray());
  }

  @Test
  void testNegativeNumbersDecreasingExponentially() {
    testAdding(IntStream.range(0, 100).mapToDouble(i -> -Math.exp(-i)).toArray());
  }

  @Test
  void testNegativeAndPositiveNumbersDecreasingExponentially() {
    testAdding(
        DoubleStream.concat(
                IntStream.range(0, 100).mapToDouble(i -> -Math.exp(-i)),
                IntStream.range(0, 100).mapToDouble(i -> Math.exp(-i)))
            .toArray());
  }

  @Override
  protected void test(boolean merged, double[] values, DDSketch sketch) {
    assertEncodes(merged, values, sketch);
    try {
      testProtoRoundTrip(merged, values, sketch);
    } catch (InvalidProtocolBufferException e) {
      fail(e);
    }
  }

  void testProtoRoundTrip(boolean merged, double[] values, DDSketch sketch)
      throws InvalidProtocolBufferException {
    assertEncodes(
        merged,
        values,
        DDSketchProtoBinding.fromProto(storeSupplier(), DDSketchProtoBinding.toProto(sketch)));
    assertEncodes(
        merged,
        values,
        DDSketchProtoBinding.fromProto(
            storeSupplier(),
            com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(sketch.serialize())));
  }

  static class DDSketchTest1 extends DDSketchTest {

    @Override
    double relativeAccuracy() {
      return 1e-1;
    }
  }

  static class DDSketchTest2 extends DDSketchTest {

    @Override
    double relativeAccuracy() {
      return 1e-2;
    }
  }

  static class DDSketchTest3 extends DDSketchTest {

    @Override
    double relativeAccuracy() {
      return 1e-3;
    }
  }
}
