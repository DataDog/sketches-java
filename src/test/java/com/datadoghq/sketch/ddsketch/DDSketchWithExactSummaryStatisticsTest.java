/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.WithExactSummaryStatistics;
import com.datadoghq.sketch.WithExactSummaryStatisticsTest;
import com.datadoghq.sketch.ddsketch.encoding.ByteArrayInput;
import com.datadoghq.sketch.ddsketch.encoding.GrowingByteArrayOutput;
import com.datadoghq.sketch.ddsketch.encoding.Input;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.StoreTestCase;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

public abstract class DDSketchWithExactSummaryStatisticsTest
    extends WithExactSummaryStatisticsTest<DDSketch, DDSketchWithExactSummaryStatistics> {

  abstract double relativeAccuracy();

  IndexMapping mapping() {
    return new LogarithmicMapping(relativeAccuracy());
  }

  Supplier<Store> storeSupplier() {
    return UnboundedSizeDenseStore::new;
  }

  @Override
  protected DDSketchWithExactSummaryStatistics newSketch() {
    return new DDSketchWithExactSummaryStatistics(() -> new DDSketch(mapping(), storeSupplier()));
  }

  @Override
  protected void assertQuantileAccurate(
      boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {
    DDSketchTest.assertQuantileAccurate(
        sortedValues, quantile, actualQuantileValue, relativeAccuracy());
  }

  @Override
  protected void test(
      boolean merged, double[] values, WithExactSummaryStatistics<DDSketch> sketch) {
    assertEncodes(merged, values, sketch);
    testEncodeDecode(merged, values, (DDSketchWithExactSummaryStatistics) sketch);
  }

  void testEncodeDecode(
      boolean merged,
      double[] values,
      DDSketchWithExactSummaryStatistics sketch,
      Supplier<Store> finalStoreSupplier) {
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch.encode(output, false);
    } catch (IOException e) {
      fail(e);
    }
    { // DDSketchWithExactSummaryStatistics -> encoded -> DDSketchWithExactSummaryStatistics
      final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
      final DDSketchWithExactSummaryStatistics decoded;
      try {
        decoded = DDSketchWithExactSummaryStatistics.decode(input, finalStoreSupplier);
        assertThat(input.hasRemaining()).isFalse();
      } catch (IOException e) {
        fail(e);
        return;
      }
      assertEncodes(merged, values, decoded);
    }
    { // DDSketchWithExactSummaryStatistics -> encoded -> DDSketch (exact summary statistics are
      // lost)
      final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
      final DDSketch decoded;
      try {
        decoded = DDSketch.decode(input, finalStoreSupplier);
        assertThat(input.hasRemaining()).isFalse();
      } catch (IOException e) {
        fail(e);
        return;
      }
      assertCountAccurate(values, decoded.getCount());
    }
  }

  void testEncodeDecode(
      boolean merged, double[] values, DDSketchWithExactSummaryStatistics sketch) {
    Arrays.stream(StoreTestCase.values())
        .filter(StoreTestCase::isLossless)
        .forEach(
            storeTestCase ->
                testEncodeDecode(merged, values, sketch, storeTestCase.storeSupplier()));
  }

  @Test
  void testDecodeAndMergeWith() {
    final double[] values = new double[] {0.33, -7};
    final DDSketchWithExactSummaryStatistics sketch0 = newSketch();
    final DDSketchWithExactSummaryStatistics sketch1 = newSketch();
    sketch0.accept(values[0]);
    sketch1.accept(values[1]);
    final GrowingByteArrayOutput output0 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    final GrowingByteArrayOutput output1 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch0.encode(output0, false);
      sketch1.encode(output1, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input0 = ByteArrayInput.wrap(output0.backingArray(), 0, output0.numWrittenBytes());
    final Input input1 = ByteArrayInput.wrap(output1.backingArray(), 0, output1.numWrittenBytes());
    final DDSketchWithExactSummaryStatistics decoded;
    try {
      decoded = DDSketchWithExactSummaryStatistics.decode(input0, storeSupplier());
      decoded.decodeAndMergeWith(input1);
      assertThat(input0.hasRemaining()).isFalse();
      assertThat(input1.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }
    assertEncodes(true, values, decoded);
  }

  @Test
  void testMergingByConcatenatingEncoded() {
    final double[] values = new double[] {0.33, -7};
    final DDSketchWithExactSummaryStatistics sketch0 = newSketch();
    final DDSketchWithExactSummaryStatistics sketch1 = newSketch();
    sketch0.accept(values[0]);
    sketch1.accept(values[1]);
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch0.encode(output, false);
      sketch1.encode(output, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
    final DDSketchWithExactSummaryStatistics decoded;
    try {
      decoded = DDSketchWithExactSummaryStatistics.decode(input, storeSupplier());
      assertThat(input.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }
    assertEncodes(true, values, decoded);
  }

  @Test
  void testMissingExactSummaryStatistics() {
    final double[] values = new double[] {0.33, -7};
    final DDSketch sketch = new DDSketch(mapping(), storeSupplier());
    Arrays.stream(values).forEach(sketch);
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch.encode(output, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> DDSketchWithExactSummaryStatistics.decode(input, storeSupplier()));
  }

  @Test
  void testBuildFromData() {
    final DDSketch emptySketch =
        new DDSketch(new LogarithmicMapping(1e-2), UnboundedSizeDenseStore::new);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> DDSketchWithExactSummaryStatistics.of(emptySketch, -1, 0, 0, 0));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> DDSketchWithExactSummaryStatistics.of(emptySketch, 0, 0, 0, 0));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                DDSketchWithExactSummaryStatistics.of(
                    emptySketch, 0, Double.MAX_VALUE, Double.MIN_VALUE, 0));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> DDSketchWithExactSummaryStatistics.of(emptySketch, 1, 1, 0, 0));
    assertThatNoException()
        .isThrownBy(
            () ->
                DDSketchWithExactSummaryStatistics.of(
                    emptySketch, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0));
  }

  static class DDSketchTestWithExactSummaryStatistics1
      extends DDSketchWithExactSummaryStatisticsTest {

    @Override
    double relativeAccuracy() {
      return 1e-1;
    }
  }
}
