/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static com.datadoghq.sketch.ddsketch.Serializer.*;

import com.datadoghq.sketch.ddsketch.Serializer;
import com.datadoghq.sketch.ddsketch.encoding.IndexMappingLayout;
import com.datadoghq.sketch.ddsketch.encoding.Output;
import java.io.IOException;
import java.util.Objects;

/**
 * A base class for mappings that are derived from a function that approximates the logarithm,
 * namely {@link #log}.
 *
 * <p>That function is scaled depending on the targeted relative accuracy, the base of the logarithm
 * that {@link #log} approximates and how well it geometrically pulls apart values from one another,
 * that is to say, the infimum of |(l∘exp)(x)-(l∘exp)(y)|/|x-y| where x ≠ y and l = {@link #log}
 */
abstract class LogLikeIndexMapping implements IndexMapping {

  private final double gamma;
  private final double indexOffset;

  // Fields precomputed for performance.
  private final double relativeAccuracy;
  private final double multiplier;

  /**
   * Constructs a mapping that approximates x -> log(x) + indexOffset, where log is to the base
   * gamma.
   *
   * @param gamma the base of the logarithm that the constructed mapping approaches
   * @param indexOffset the value such that {@code logInverse(indexOffset / multiplier)} is the left
   *     bound of the bucket of index 0
   */
  LogLikeIndexMapping(double gamma, double indexOffset) {
    this.gamma = requireValidGamma(gamma);
    this.indexOffset = indexOffset;
    this.multiplier = Math.log(base()) / Math.log1p(gamma - 1);
    this.relativeAccuracy = relativeAccuracy(gamma, correctingFactor());
  }

  private static double relativeAccuracy(double gamma, double correctingFactor) {
    double correctedGamma = Math.pow(gamma, correctingFactor);
    return (correctedGamma - 1) / (correctedGamma + 1);
  }

  static double gamma(double relativeAccuracy, double correctingFactor) {
    return Math.pow((1 + relativeAccuracy) / (1 - relativeAccuracy), 1 / correctingFactor);
  }

  static double requireValidRelativeAccuracy(double relativeAccuracy) {
    if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
      throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
    }
    return relativeAccuracy;
  }

  private static double requireValidGamma(double gamma) {
    if (gamma <= 1) {
      throw new IllegalArgumentException("gamma must be greater than 1.");
    }
    return gamma;
  }

  /** @return an approximation of {@code Math.log(value) / Math.log(base())} */
  abstract double log(double value);

  /**
   * The exact inverse of {@link #log}.
   *
   * @return the {@code value} such that {@code log(value) == index}
   */
  abstract double logInverse(double index);

  /** @return the base of the logarithm that {@link #log} approaches */
  abstract double base();

  /**
   * @return a factor that corrects the fact that {@code log} may not geometrically pull apart
   *     values from one another as well as the logarithm; it is equal to the inverse of the infimum
   *     of log(b)⋅|(l∘exp)(x)-(l∘exp)(y)|/|x-y| where x ≠ y, b = {@link #base} and l = {@link #log}
   */
  abstract double correctingFactor();

  @Override
  public final int index(double value) {
    final double index = log(value) * multiplier + indexOffset;
    return index >= 0 ? (int) index : (int) index - 1; // faster than Math::floor
  }

  @Override
  public final double value(int index) {
    return lowerBound(index) * (1 + relativeAccuracy);
  }

  @Override
  public double lowerBound(int index) {
    return logInverse((index - indexOffset) / multiplier);
  }

  @Override
  public double upperBound(int index) {
    return lowerBound(index + 1);
  }

  @Override
  public final double relativeAccuracy() {
    return relativeAccuracy;
  }

  @Override
  public double minIndexableValue() {
    return Math.max(
        Math.pow(
            base(),
            (Integer.MIN_VALUE - indexOffset) / multiplier
                + 1), // so that index >= Integer.MIN_VALUE
        Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy));
  }

  @Override
  public double maxIndexableValue() {
    return Math.min(
        Math.pow(
            base(),
            (Integer.MAX_VALUE - indexOffset) / multiplier
                - 1), // so that index <= Integer.MAX_VALUE
        Double.MAX_VALUE / (1 + relativeAccuracy));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LogLikeIndexMapping that = (LogLikeIndexMapping) o;
    return Double.compare(that.gamma, gamma) == 0
        && Double.compare(that.indexOffset, indexOffset) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(gamma, indexOffset);
  }

  abstract IndexMappingLayout layout();

  @Override
  public void encode(Output output) throws IOException {
    layout().toFlag().encode(output);
    output.writeDoubleLE(gamma);
    output.writeDoubleLE(indexOffset);
  }

  abstract Interpolation interpolation();

  @Override
  public int serializedSize() {
    return doubleFieldSize(1, gamma)
        + doubleFieldSize(2, indexOffset)
        + fieldSize(3, interpolation().ordinal());
  }

  @Override
  public void serialize(Serializer serializer) {
    serializer.writeDouble(1, gamma);
    serializer.writeDouble(2, indexOffset);
    serializer.writeUnsignedInt32(3, interpolation().ordinal());
  }

  double gamma() {
    return gamma;
  }

  double indexOffset() {
    return indexOffset;
  }
}
