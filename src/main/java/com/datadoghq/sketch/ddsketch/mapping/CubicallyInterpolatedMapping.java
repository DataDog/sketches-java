package com.datadoghq.sketch.ddsketch.mapping;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link LogarithmicMapping}) by
 * extracting the floor value of the logarithm to the base 2 from the binary representations of floating-point values
 * and cubically interpolating the logarithm in-between.
 */
public class CubicallyInterpolatedMapping extends LogLikeIndexMapping {

    // Assuming we write the index as index(v) = floor(multiplier*ln(2)/ln(gamma)*(e+As^3+Bs^2+Cs)), where v=2^e(1+s)
    // and gamma = (1+relativeAccuracy)/(1-relativeAccuracy), those are the coefficients that minimize the multiplier,
    // therefore the memory footprint of the sketch, while ensuring the relative accuracy of the sketch.
    private static final double A = 6.0 / 35.0;
    private static final double B = -3.0 / 5.0;
    private static final double C = 10.0 / 7.0;

    public CubicallyInterpolatedMapping(double relativeAccuracy) {
        super(relativeAccuracy);
    }

    /**
     * {@inheritDoc}
     */
    CubicallyInterpolatedMapping(double gamma, double indexOffset) {
        super(gamma, indexOffset);
    }

    @Override
    double log(double value) {
        final long longBits = Double.doubleToRawLongBits(value);
        final double s = DoubleBitOperationHelper.getSignificandPlusOne(longBits) - 1;
        final double e = (double) DoubleBitOperationHelper.getExponent(longBits);
        return ((A * s + B) * s + C) * s + e;
    }

    @Override
    double logInverse(double index) {
        final long exponent = (long) Math.floor(index);
        // Derived from Cardano's formula
        final double d0 = B * B - 3 * A * C;
        final double d1 = 2 * B * B * B - 9 * A * B * C - 27 * A * A * (index - exponent);
        final double p = Math.cbrt((d1 - Math.sqrt(d1 * d1 - 4 * d0 * d0 * d0)) / 2);
        final double significandPlusOne = -(B + p + d0 / p) / (3 * A) + 1;
        return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne);
    }

    @Override
    double base() {
        return 2;
    }

    @Override
    double correctingFactor() {
        return 7 / (10 * Math.log(2));
    }
}
