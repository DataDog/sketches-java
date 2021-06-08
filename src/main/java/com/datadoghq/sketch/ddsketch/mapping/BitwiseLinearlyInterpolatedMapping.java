/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static com.datadoghq.sketch.ddsketch.Serializer.*;
import static com.datadoghq.sketch.ddsketch.mapping.Interpolation.LINEAR;

import com.datadoghq.sketch.ddsketch.Serializer;
import java.util.Objects;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link
 * LogarithmicMapping}) by extracting the floor value of the logarithm to the base 2 from the binary
 * representations of floating-point values and linearly interpolating the logarithm in-between
 * using, again, the binary representation.
 *
 * <p>Note that this mapping, while fast, might be highly memory inefficient as it only works for a
 * discrete set of relative accuracies and will fall back to the largest possible one that is less
 * than the requested accuracy (which can be as low as half of it, in the worst case).
 */
public class BitwiseLinearlyInterpolatedMapping implements IndexMapping {

  private final int numSignificantBinaryDigits;
  private final int partialSignificandShift;
  private final int multiplier;
  private final double relativeAccuracy;

  public BitwiseLinearlyInterpolatedMapping(double relativeAccuracy) {
    this(getMinNumSignificantBinaryDigits(relativeAccuracy));
  }

  BitwiseLinearlyInterpolatedMapping(int numSignificantBinaryDigits) {
    if (numSignificantBinaryDigits < 0) {
      throw new IllegalArgumentException(
          "The number of significant binary digits cannot be negative.");
    }
    this.numSignificantBinaryDigits = numSignificantBinaryDigits;
    this.partialSignificandShift =
        DoubleBitOperationHelper.SIGNIFICAND_WIDTH - numSignificantBinaryDigits - 1;
    this.multiplier = 1 << numSignificantBinaryDigits;
    this.relativeAccuracy = 1 - 2 / (1 + Math.exp(1.0 / this.multiplier));
  }

  /**
   * @return the minimum number of significant binary digits that are required for the relative
   *     accuracy to be guaranteed
   */
  private static int getMinNumSignificantBinaryDigits(double relativeAccuracy) {
    if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
      throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
    }
    final double multiplier = 1 / Math.log1p(2 * relativeAccuracy / (1 - relativeAccuracy));
    return Math.max((int) Math.ceil(Math.log(multiplier) / Math.log(2)), 0);
  }

  @Override
  public int index(double value) {
    final long longBits = Double.doubleToRawLongBits(value);
    return (int)
        ((DoubleBitOperationHelper.getExponent(longBits) << numSignificantBinaryDigits)
            | getPartialSignificand(longBits));
  }

  private long getPartialSignificand(long longBits) {
    return (longBits & DoubleBitOperationHelper.SIGNIFICAND_MASK) >> partialSignificandShift;
  }

  @Override
  public double value(int index) {
    return lowerBound(index) * (1 + relativeAccuracy);
  }

  @Override
  public double lowerBound(int index) {
    final int exponent = Math.floorDiv(index, multiplier);
    return DoubleBitOperationHelper.buildDouble(
        exponent, 1 - exponent + (double) index / multiplier);
  }

  @Override
  public double upperBound(int index) {
    return lowerBound(index + 1);
  }

  @Override
  public double relativeAccuracy() {
    return relativeAccuracy;
  }

  @Override
  public double minIndexableValue() {
    return Math.max(
        Math.pow(2, Integer.MIN_VALUE / multiplier + 1), // so that index >= Integer.MIN_VALUE
        Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy));
  }

  @Override
  public double maxIndexableValue() {
    return Math.min(
        Math.pow(2, Integer.MAX_VALUE / multiplier), // so that index <= Integer.MAX_VALUE
        Double.MAX_VALUE / (1 + relativeAccuracy));
  }

  @Override
  public int serializedSize() {
    return doubleFieldSize(1, gamma()) + fieldSize(3, LINEAR.ordinal());
  }

  @Override
  public void serialize(Serializer serializer) {
    serializer.writeDouble(1, gamma());
    serializer.writeUnsignedInt32(3, LINEAR.ordinal());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return numSignificantBinaryDigits
        == ((BitwiseLinearlyInterpolatedMapping) o).numSignificantBinaryDigits;
  }

  @Override
  public int hashCode() {
    return Objects.hash(numSignificantBinaryDigits);
  }

  double gamma() {
    return Math.pow(2, 1.0 / multiplier);
  }
}
