package com.datadoghq.sketch.ddsketch.mapping;

import java.util.Objects;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link LogarithmicMapping}) by
 * extracting the floor value of the logarithm to the base 2 from the binary representations of floating-point values
 * and cubically interpolating the logarithm in-between.
 */
public class CubicallyInterpolatedMapping implements IndexMapping {

    // Assuming we write the index as index(v) = floor(multiplier*ln(2)/ln(gamma)*(e+As^3+Bs^2+Cs)), where v=2^e(1+s)
    // and gamma = (1+relativeAccuracy)/(1-relativeAccuracy), those are the coefficients that minimize the multiplier,
    // therefore the memory footprint of the sketch, while ensuring the relative accuracy of the sketch.
    private static final double A = 6.0 / 35.0;
    private static final double B = -3.0 / 5.0;
    private static final double C = 10.0 / 7.0;

    private final double relativeAccuracy;
    private final double multiplier;

    public CubicallyInterpolatedMapping(double relativeAccuracy) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        this.relativeAccuracy = relativeAccuracy;
        this.multiplier = 7.0 / (10.0 * Math.log((1 + relativeAccuracy) / (1 - relativeAccuracy)));
    }

    @Override
    public int index(double value) {
        final long longBits = Double.doubleToRawLongBits(value);
        final double s = DoubleBitOperationHelper.getSignificandPlusOne(longBits) - 1;
        final double e = (double) DoubleBitOperationHelper.getExponent(longBits);
        final double index = (((A * s + B) * s + C) * s + e) * multiplier;
        return index >= 0 ? (int) index : (int) index - 1;
    }

    @Override
    public double value(int index) {
        final double normalizedIndex = (double) index / multiplier;
        final long exponent = (long) Math.floor(normalizedIndex);
        // Derived from Cardano's formula
        final double d0 = B * B - 3 * A * C;
        final double d1 = 2 * B * B * B - 9 * A * B * C - 27 * A * A * (normalizedIndex - exponent);
        final double p = Math.cbrt((d1 - Math.sqrt(d1 * d1 - 4 * d0 * d0 * d0)) / 2);
        final double significandPlusOne = -(B + p + d0 / p) / (3 * A) + 1;
        return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne) * (1 + relativeAccuracy);
    }

    @Override
    public double relativeAccuracy() {
        return relativeAccuracy;
    }

    @Override
    public double minIndexableValue() {
        return Math.max(
                Math.pow(2, (Integer.MIN_VALUE + 1) / multiplier + 1), // so that index >= Integer.MIN_VALUE
                Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy)
        );
    }

    @Override
    public double maxIndexableValue() {
        return Math.min(
                Math.pow(2, Integer.MAX_VALUE / multiplier - 1), // so that index <= Integer.MAX_VALUE
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
        return Double.compare(relativeAccuracy, ((CubicallyInterpolatedMapping) o).relativeAccuracy) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativeAccuracy);
    }
}
