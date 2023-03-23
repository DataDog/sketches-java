/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import com.datadoghq.sketch.ddsketch.encoding.IndexMappingLayout;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link
 * LogarithmicMapping}) by extracting the floor value of the logarithm to the base 2 from the binary
 * representations of floating-point values and quartically interpolating the logarithm in-between.
 *
 * <p>The optimal polynomial coefficients can be calculated using the method described in {@link
 * CubicallyInterpolatedMapping}.
 */
public class QuarticallyInterpolatedMapping extends LogLikeIndexMapping {

  private static final double A = -2.0 / 25.0;
  private static final double B = 8.0 / 25.0;
  private static final double C = -17.0 / 25.0;
  private static final double D = 36.0 / 25.0;

  private static final double CORRECTING_FACTOR = 1 / (D * Math.log(2));

  public QuarticallyInterpolatedMapping(double relativeAccuracy) {
    super(gamma(requireValidRelativeAccuracy(relativeAccuracy), CORRECTING_FACTOR), 0);
  }

  /** {@inheritDoc} */
  public QuarticallyInterpolatedMapping(double gamma, double indexOffset) {
    super(gamma, indexOffset);
  }

  @Override
  double log(double value) {
    final long longBits = Double.doubleToRawLongBits(value);
    final double s = DoubleBitOperationHelper.getSignificandPlusOne(longBits) - 1;
    final double e = (double) DoubleBitOperationHelper.getExponent(longBits);
    return (((A * s + B) * s + C) * s + D) * s + e;
  }

  @Override
  double logInverse(double index) {
    final double exponent = Math.floor(index);
    final double e = exponent - index;
    // Derived from Ferrari's method
    final double alpha = -(3 * B * B) / (8 * A * A) + C / A; // 2.5
    final double beta = (B * B * B) / (8 * A * A * A) - (B * C) / (2 * A * A) + D / A; // -9.0
    final double gamma =
        -(3 * B * B * B * B) / (256 * A * A * A * A)
            + (C * B * B) / (16 * A * A * A)
            - (B * D) / (4 * A * A)
            + e / A;
    final double p = -(alpha * alpha) / 12 - gamma;
    final double q = -(alpha * alpha * alpha) / 108 + (alpha * gamma) / 3 - (beta * beta) / 8;
    final double r = -q / 2 + Math.sqrt((q * q) / 4 + (p * p * p) / 27);
    final double u = Math.cbrt(r);
    final double y = -(5 * alpha) / 6 + u - p / (3 * u);
    final double w = Math.sqrt(alpha + 2 * y);
    final double x = -B / (4 * A) + (w - Math.sqrt(-(3 * alpha + 2 * y + (2 * beta) / w))) / 2;
    return DoubleBitOperationHelper.buildDouble((long) exponent, x + 1);
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
    return IndexMappingLayout.LOG_QUARTIC;
  }

  @Override
  Interpolation interpolation() {
    return Interpolation.QUARTIC;
  }
}
