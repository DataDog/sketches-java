/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link LogarithmicMapping}) by
 * extracting the floor value of the logarithm to the base 2 from the binary representations of floating-point values
 * and linearly interpolating the logarithm in-between.
 */
public class LinearlyInterpolatedMapping extends LogLikeIndexMapping {

    public LinearlyInterpolatedMapping(double relativeAccuracy) {
        super(relativeAccuracy);
    }

    /**
     * {@inheritDoc}
     */
    LinearlyInterpolatedMapping(double gamma, double indexOffset) {
        super(gamma, indexOffset);
    }

    @Override
    double log(double value) {
        final long longBits = Double.doubleToRawLongBits(value);
        return (double) DoubleBitOperationHelper.getExponent(longBits)
            + DoubleBitOperationHelper.getSignificandPlusOne(longBits);
    }

    @Override
    double logInverse(double index) {
        final long exponent = (long) Math.floor(index - 1);
        final double significandPlusOne = index - exponent;
        return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne);
    }

    @Override
    double base() {
        return 2;
    }

    @Override
    double correctingFactor() {
        return 1 / Math.log(2);
    }
}
