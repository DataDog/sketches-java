/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch;

import java.util.function.DoubleConsumer;

/**
 * A data structure that consumes {@code double} values and can compute quantiles over the ingested
 * values.
 *
 * <p>Quantile values are usually computed with an approximation error that depends on the sketch
 * implementation.
 */
public interface QuantileSketch<QS extends QuantileSketch<QS>> extends DoubleConsumer {

  /**
   * Adds a value to the sketch.
   *
   * @param value the value to be added
   */
  @Override
  void accept(double value);

  /**
   * Adds a value to the sketch with a floating-point {@code count}.
   *
   * <p>If {@code count} is an integer, calling {@code accept(value, count)} is equivalent to
   * calling {@code accept(value)} {@code count} times.
   *
   * @param value the value to be added
   * @param count the weight associated with the value to be added
   * @throws IllegalArgumentException if {@code count} is negative
   */
  void accept(double value, double count);

  /**
   * Merges the other sketch into this one. After this operation, this sketch encodes the values
   * that were added to both this and the other sketches.
   *
   * @param other the sketch to be merged into this one
   * @throws NullPointerException if {@code other} is {@code null}
   */
  void mergeWith(QS other);

  /** @return a (deep) copy of this sketch */
  QS copy();

  /** @return iff no value has been added to this sketch */
  default boolean isEmpty() {
    return getCount() == 0;
  }

  /**
   * Sets all counts in the sketch to zero. The sketch behaves as if it were empty after this call,
   * but any allocated memory is not released.
   */
  void clear();

  /** @return the total number of values that have been added to this sketch */
  double getCount();

  /**
   * @return the minimum value that has been added to this sketch
   * @throws java.util.NoSuchElementException if the sketch is empty
   */
  default double getMinValue() {
    return getValueAtQuantile(0);
  }

  /**
   * @return the maximum value that has been added to this sketch
   * @throws java.util.NoSuchElementException if the sketch is empty
   */
  default double getMaxValue() {
    return getValueAtQuantile(1);
  }

  /**
   * @param quantile a number between 0 and 1 (both included)
   * @return the value at the specified quantile
   * @throws java.util.NoSuchElementException if the sketch is empty
   */
  double getValueAtQuantile(double quantile);

  /**
   * @param quantiles number between 0 and 1 (both included)
   * @return the values at the respective specified quantiles
   * @throws java.util.NoSuchElementException if the sketch is empty
   */
  double[] getValuesAtQuantiles(double[] quantiles);
}
