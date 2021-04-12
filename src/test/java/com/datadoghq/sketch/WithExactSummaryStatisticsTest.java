/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Assertions;

public class WithExactSummaryStatisticsTest
    extends QuantileSketchTest<
        WithExactSummaryStatistics<WithExactSummaryStatisticsTest.DummySketch>> {

  static class DummySketch implements QuantileSketch<DummySketch> {

    private double count;

    private DummySketch() {
      this.count = 0;
    }

    private DummySketch(double count) {
      this.count = count;
    }

    @Override
    public void accept(double value) {
      count++;
    }

    @Override
    public void accept(double value, double count) {
      if (count < 0) {
        throw new IllegalArgumentException();
      }
      this.count += count;
    }

    @Override
    public void mergeWith(DummySketch other) {
      this.count += other.count;
    }

    @Override
    public DummySketch copy() {
      return new DummySketch(count);
    }

    @Override
    public void clear() {
      count = 0;
    }

    @Override
    public double getCount() {
      // This method should not be relied upon.
      Assertions.fail();
      return 0;
    }

    @Override
    public double getSum() {
      // This method should not be relied upon.
      Assertions.fail();
      return 0;
    }

    @Override
    public double getMinValue() {
      // This method should not be relied upon.
      Assertions.fail();
      return 0;
    }

    @Override
    public double getMaxValue() {
      // This method should not be relied upon.
      Assertions.fail();
      return 0;
    }

    @Override
    public double getValueAtQuantile(double quantile) {
      if (quantile < 0 || quantile > 1) {
        throw new IllegalArgumentException();
      }
      if (count == 0) {
        throw new NoSuchElementException();
      }
      return 0;
    }

    @Override
    public double[] getValuesAtQuantiles(double[] quantiles) {
      if (Arrays.stream(quantiles).anyMatch(quantile -> quantile < 0 || quantile > 1)) {
        throw new IllegalArgumentException();
      }
      if (count == 0) {
        throw new NoSuchElementException();
      }
      return new double[] {0};
    }
  }

  @Override
  protected WithExactSummaryStatistics<DummySketch> newSketch() {
    return new WithExactSummaryStatistics<>(DummySketch::new);
  }

  @Override
  protected void assertQuantileAccurate(
      boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {
    // Nothing to assert
  }

  @Override
  protected void assertMinAccurate(double[] sortedValues, double actualMinValue) {
    assertEquals(sortedValues[0], actualMinValue);
  }

  @Override
  protected void assertMaxAccurate(double[] sortedValues, double actualMaxValue) {
    assertEquals(sortedValues[sortedValues.length - 1], actualMaxValue);
  }

  @Override
  protected void assertSumAccurate(double[] sortedValues, double actualSumValue) {
    final DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
    Arrays.stream(sortedValues).forEach(summaryStatistics);
    assertEquals(summaryStatistics.getSum(), actualSumValue);
  }

  @Override
  protected void assertAverageAccurate(double[] sortedValues, double actualAverageValue) {
    final DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
    Arrays.stream(sortedValues).forEach(summaryStatistics);
    assertEquals(summaryStatistics.getAverage(), actualAverageValue);
  }
}
