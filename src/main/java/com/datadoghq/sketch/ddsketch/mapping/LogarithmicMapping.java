/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

/**
 * An {@link IndexMapping} that is <i>memory-optimal</i>, that is to say that given a targeted relative accuracy, it
 * requires the least number of indices to cover a given range of values. This is done by logarithmically mapping
 * floating-point values to integers.
 */
public class LogarithmicMapping extends LogLikeIndexMapping {

    public LogarithmicMapping(double relativeAccuracy) {
        super(relativeAccuracy);
    }

    /**
     * {@inheritDoc}
     */
    LogarithmicMapping(double gamma, double indexOffset) {
        super(gamma, indexOffset);
    }

    @Override
    double log(double value) {
        return Math.log(value);
    }

    @Override
    double logInverse(double index) {
        return Math.exp(index);
    }

    @Override
    double base() {
        return Math.E;
    }

    @Override
    double correctingFactor() {
        return 1;
    }

    @Override
    com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation interpolationToProto() {
        return com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.NONE;
    }
}
