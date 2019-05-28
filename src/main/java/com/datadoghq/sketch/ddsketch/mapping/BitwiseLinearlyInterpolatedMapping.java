package com.datadoghq.sketch.ddsketch.mapping;

import java.util.Objects;

/**
 * Computation-optimized
 */
public class BitwiseLinearlyInterpolatedMapping implements IndexMapping {

    private final int numSignificantBinaryDigits;
    private final int partialSignificandShift;
    private final int multiplier;
    private final double relativeAccuracy;

    public BitwiseLinearlyInterpolatedMapping(double relativeAccuracy) {
        this(getMinNumSignificantBinaryDigits(relativeAccuracy));
    }

    public BitwiseLinearlyInterpolatedMapping(int numSignificantBinaryDigits) {
        if (numSignificantBinaryDigits < 0) {
            throw new IllegalArgumentException("The number of significant binary digits cannot be negative.");
        }
        this.numSignificantBinaryDigits = numSignificantBinaryDigits;
        this.partialSignificandShift = DoubleBitOperationHelper.SIGNIFICAND_WIDTH - numSignificantBinaryDigits - 1;
        this.multiplier = 1 << numSignificantBinaryDigits;
        this.relativeAccuracy = 1 - 1 / (1 + Math.pow(2, -(numSignificantBinaryDigits + 1)));
    }

    /**
     * @return the minimum number of significant binary digits that are required for the relative accuracy to be guaranteed
     */
    private static int getMinNumSignificantBinaryDigits(double relativeAccuracy) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        return Math.max((int) Math.ceil(Math.log(1 / relativeAccuracy - 1) / Math.log(2) - 1), 0);
    }

    @Override
    public int index(double value) {
        final long longBits = Double.doubleToRawLongBits(value);
        return (int) ((DoubleBitOperationHelper.getExponent(longBits) << numSignificantBinaryDigits) | getPartialSignificand(longBits)); //TODO perf
    }

    private long getPartialSignificand(long longBits) {
        return (longBits & DoubleBitOperationHelper.SIGNIFICAND_MASK) >> partialSignificandShift;
    }

    @Override
    public double value(int index) {
        final int exponent = Math.floorDiv(index, multiplier);
        return DoubleBitOperationHelper.buildDouble(
                exponent,
                1 - exponent + (double) index / multiplier
        ) * (1 + relativeAccuracy);
    }

    @Override
    public double relativeAccuracy() {
        return relativeAccuracy;
    }

    @Override
    public double minIndexableValue() {
        return Math.max(
                Math.pow(2, Integer.MIN_VALUE / multiplier + 1), // so that index >= Integer.MIN_VALUE
                Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy)
        );
    }

    @Override
    public double maxIndexableValue() {
        return Math.min(
                Math.pow(2, Integer.MAX_VALUE / multiplier), // so that index <= Integer.MAX_VALUE
                Double.MAX_VALUE / (1 + relativeAccuracy)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return numSignificantBinaryDigits == ((BitwiseLinearlyInterpolatedMapping) o).numSignificantBinaryDigits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSignificantBinaryDigits);
    }
}
