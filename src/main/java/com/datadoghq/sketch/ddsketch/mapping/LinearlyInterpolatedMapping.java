/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static com.datadoghq.sketch.ddsketch.mapping.Interpolation.LINEAR;

import com.datadoghq.sketch.ddsketch.encoding.IndexMappingLayout;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link
 * LogarithmicMapping}) by extracting the floor value of the logarithm to the base 2 from the binary
 * representations of floating-point values and linearly interpolating the logarithm in-between.
 */
public class LinearlyInterpolatedMapping extends LogLikeIndexMapping {

  private static final double CORRECTING_FACTOR = 1 / Math.log(2);

  public LinearlyInterpolatedMapping(double relativeAccuracy) {
    super(
        gamma(requireValidRelativeAccuracy(relativeAccuracy), CORRECTING_FACTOR),
        indexOffsetShift(relativeAccuracy));
  }

  /** {@inheritDoc} */
  LinearlyInterpolatedMapping(double gamma, double indexOffset) {
    super(gamma, indexOffset);
  }

  /**
   * Contrary to other mappings, {@link
   * LinearlyInterpolatedMapping#LinearlyInterpolatedMapping(double)} do not map 1 to 0. This method
   * returns the index offset shift to maintain backward compatibility.
   */
  private static double indexOffsetShift(double relativeAccuracy) {
    return 1 / (Math.log1p(2 * relativeAccuracy / (1 - relativeAccuracy)));
  }

  @Override
  double log(double value) {
    final long longBits = Double.doubleToRawLongBits(value);
    return (double) DoubleBitOperationHelper.getExponent(longBits)
        + DoubleBitOperationHelper.getSignificandPlusOne(longBits)
        - 1;
  }

  @Override
  double logInverse(double index) {
    final long exponent = (long) Math.floor(index);
    final double significandPlusOne = index - exponent + 1;
    return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne);
  }

  @Override
  double base() {
    return 2;
  }

  @Override
  double correctingFactor() {
    return CORRECTING_FACTOR;
  }

  @Override
  IndexMappingLayout layout() {
    return IndexMappingLayout.LOG_LINEAR;
  }

  @Override
  Interpolation interpolation() {
    return LINEAR;
  }
}
