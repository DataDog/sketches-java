/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link LogarithmicMapping}) by
 * extracting the floor value of the logarithm to the base 2 from the binary representations of floating-point values
 * and quadratically interpolating the logarithm in-between.
 */
public class QuadraticallyInterpolatedMapping extends LogLikeIndexMapping {

    private static final double ONE_THIRD = 1.0 / 3.0;

    public QuadraticallyInterpolatedMapping(double relativeAccuracy) {
        super(relativeAccuracy);
    }

    /**
     * {@inheritDoc}
     */
    QuadraticallyInterpolatedMapping(double gamma, double indexOffset) {
        super(gamma, indexOffset);
    }

    @Override
    double log(double value) {
        final long longBits = Double.doubleToRawLongBits(value);
        final double s = DoubleBitOperationHelper.getSignificandPlusOne(longBits);
        final double e = DoubleBitOperationHelper.getExponent(longBits);
        return e - (s - 5) * (s - 1) * ONE_THIRD;
    }

    @Override
    double logInverse(double index) {
        final long exponent = (long) Math.floor(index);
        final double significandPlusOne = 3 - Math.sqrt(4 - 3 * (index - exponent));
        return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne);
    }

    @Override
    double base() {
        return 2;
    }

    @Override
    double correctingFactor() {
        return 3 / (4 * Math.log(2));
    }

    @Override
    com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation interpolationToProto() {
        return com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.QUADRATIC;
    }
}
