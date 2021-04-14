/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A wrapper that returns exact count, sum, average, minimum and maximum, and relies on the provided
 * {@link QuantileSketch} to keep track of quantiles.
 *
 * @param <QS> the type of the wrapped sketch that is used to compute quantiles
 */
public class WithExactSummaryStatistics<QS extends QuantileSketch<QS>>
    implements QuantileSketch<WithExactSummaryStatistics<QS>> {

  private final QS sketch;
  private double count;
  private double sum;
  // Similar to DoubleSummaryStatistics
  // We use a compensated sum to avoid accumulating rounding errors.
  // See https://en.wikipedia.org/wiki/Kahan_summation_algorithm.
  private double sumCompensation; // Low order bits of sum
  private double simpleSum; // Used to compute right sum for non-finite inputs
  private double min;
  private double max;

  public WithExactSummaryStatistics(Supplier<QS> sketchSupplier) {
    this.sketch = sketchSupplier.get();
    this.count = 0;
    this.sum = 0;
    this.sumCompensation = 0;
    this.simpleSum = 0;
    this.min = Double.POSITIVE_INFINITY;
    this.max = Double.NEGATIVE_INFINITY;
  }

  private WithExactSummaryStatistics(
      QS sketch,
      double count,
      double sum,
      double sumCompensation,
      double simpleSum,
      double min,
      double max) {
    this.sketch = sketch;
    this.count = count;
    this.sum = sum;
    this.sumCompensation = sumCompensation;
    this.simpleSum = simpleSum;
    this.min = min;
    this.max = max;
  }

  @Override
  public void accept(double value) {
    sketch.accept(value);
    count++;
    simpleSum += value;
    sumWithCompensation(value);
    min = Math.min(min, value);
    max = Math.max(max, value);
  }

  @Override
  public void accept(double value, double count) {
    sketch.accept(value, count);
    this.count += count;
    simpleSum += value * count;
    sumWithCompensation(value * count);
    min = Math.min(min, value);
    max = Math.max(max, value);
  }

  @Override
  public void mergeWith(WithExactSummaryStatistics<QS> other) {
    sketch.mergeWith(other.sketch);
    count += other.count;
    simpleSum += other.simpleSum;
    sumWithCompensation(other.sum);
    sumWithCompensation(other.sumCompensation);
    min = Math.min(min, other.min);
    max = Math.max(max, other.max);
  }

  private void sumWithCompensation(double value) {
    final double tmp = value - sumCompensation;
    final double velvel = sum + tmp; // Little wolf of rounding error
    sumCompensation = (velvel - sum) - tmp;
    sum = velvel;
  }

  @Override
  public WithExactSummaryStatistics<QS> copy() {
    return new WithExactSummaryStatistics<>(
        sketch.copy(), count, sum, sumCompensation, simpleSum, min, max);
  }

  @Override
  public void clear() {
    sketch.clear();
    count = 0;
    sum = 0;
    sumCompensation = 0;
    simpleSum = 0;
    min = Double.POSITIVE_INFINITY;
    max = Double.NEGATIVE_INFINITY;
  }

  @Override
  public double getCount() {
    return count;
  }

  @Override
  public double getSum() {
    // Better error bounds to add both terms as the final sum
    final double tmp = sum + sumCompensation;
    if (Double.isNaN(tmp) && Double.isInfinite(simpleSum)) {
      // If the compensated sum is spuriously NaN from accumulating one or more same-signed infinite
      // values, return the correctly-signed infinity stored in simpleSum.
      return simpleSum;
    } else {
      return tmp;
    }
  }

  @Override
  public double getMinValue() {
    if (count == 0) {
      throw new NoSuchElementException();
    }
    return min;
  }

  @Override
  public double getMaxValue() {
    if (count == 0) {
      throw new NoSuchElementException();
    }
    return max;
  }

  @Override
  public double getValueAtQuantile(double quantile) {
    return clamp(sketch.getValueAtQuantile(quantile));
  }

  @Override
  public double[] getValuesAtQuantiles(double[] quantiles) {
    final double[] valuesAtQuantiles = sketch.getValuesAtQuantiles(quantiles);
    for (int i = 0; i < valuesAtQuantiles.length; i++) {
      valuesAtQuantiles[i] = clamp(valuesAtQuantiles[i]);
    }
    return valuesAtQuantiles;
  }

  private double clamp(double value) {
    if (max < min) {
      // Only if the sketch is empty, in which case this method should not be called.
      throw new IllegalStateException();
    }
    return Math.max(Math.min(value, max), min);
  }
}
