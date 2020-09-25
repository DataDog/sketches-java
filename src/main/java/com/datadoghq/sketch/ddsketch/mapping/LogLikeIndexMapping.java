/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import java.util.Objects;

/**
 * A base class for mappings that are derived from a function that approximates the logarithm, namely {@link #log}.
 * <p>
 * That function is scaled depending on the targeted relative accuracy, the base of the logarithm that {@link #log}
 * approximates and how well it geometrically pulls apart values from one another, that is to say, the infimum of
 * |(l∘exp)(x)-(l∘exp)(y)|/|x-y| where x ≠ y and l = {@link #log}
 */
abstract class LogLikeIndexMapping implements IndexMapping {

    private final double relativeAccuracy;
    private final double multiplier;
    private final double normalizedIndexOffset;

    LogLikeIndexMapping(double relativeAccuracy) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        this.relativeAccuracy = relativeAccuracy;
        this.multiplier = correctingFactor() * Math.log(base())
            / (Math.log((1 + relativeAccuracy) / (1 - relativeAccuracy)));
        this.normalizedIndexOffset = 0;
    }

    /**
     * Constructs a mapping that approximates x -> log(x) + indexOffset, where log is to the base gamma.
     *
     * @param gamma       the base of the logarithm that the constructed mapping approaches
     * @param indexOffset the value such that {@code logInverse(indexOffset / multiplier)} is the left bound of the
     *                    bucket of index 0
     */
    LogLikeIndexMapping(double gamma, double indexOffset) {
        if (gamma <= 1) {
            throw new IllegalArgumentException("gamma must be greater than 1.");
        }
        this.relativeAccuracy = 1 - 2 / (1 + Math.exp(correctingFactor() * Math.log(gamma)));
        this.multiplier = Math.log(base()) / Math.log(gamma);
        this.normalizedIndexOffset = indexOffset - log(1) * this.multiplier;
    }

    /**
     * @return an approximation of {@code log(1) + Math.log(value) / Math.log(base())}
     */
    abstract double log(double value);

    /**
     * The exact inverse of {@link #log}.
     *
     * @return the {@code value} such that {@code log(value) == index}
     */
    abstract double logInverse(double index);

    /**
     * @return the base of the logarithm that {@link #log} approaches
     */
    abstract double base();

    /**
     * @return a factor that corrects the fact that {@code log} may not geometrically pull apart values from one another
     * as well as the logarithm; it is equal to the inverse of the infimum of log(b)⋅|(l∘exp)(x)-(l∘exp)(y)|/|x-y| where
     * x ≠ y, b = {@link #base} and l = {@link #log}
     */
    abstract double correctingFactor();

    @Override
    public final int index(double value) {
        final double index = log(value) * multiplier + normalizedIndexOffset;
        return index >= 0 ? (int) index : (int) index - 1; // faster than Math::floor
    }

    @Override
    public final double value(int index) {
        return logInverse((index - normalizedIndexOffset) / multiplier) * (1 + relativeAccuracy);
    }

    @Override
    public final double relativeAccuracy() {
        return relativeAccuracy;
    }

    @Override
    public double minIndexableValue() {
        return Math.max(
            Math.pow(base(), (Integer.MIN_VALUE - normalizedIndexOffset) / multiplier - log(1) + 1), // so that index >= Integer.MIN_VALUE
            Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy)
        );
    }

    @Override
    public double maxIndexableValue() {
        return Math.min(
            Math.pow(base(), (Integer.MAX_VALUE - normalizedIndexOffset) / multiplier - log(1) - 1), // so that index <= Integer.MAX_VALUE
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
        final LogLikeIndexMapping that = (LogLikeIndexMapping) o;
        return Double.compare(that.multiplier, multiplier) == 0 &&
            Double.compare(that.normalizedIndexOffset, normalizedIndexOffset) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(multiplier, normalizedIndexOffset);
    }

    abstract com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation interpolationToProto();

    @Override
    public com.datadoghq.sketch.ddsketch.proto.IndexMapping toProto() {
        return com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                .setGamma(Math.pow(base(), 1 / multiplier))
                .setIndexOffset(normalizedIndexOffset + log(1) * multiplier)
                .setInterpolation(interpolationToProto())
                .build();
    }

    static LogLikeIndexMapping fromProto(com.datadoghq.sketch.ddsketch.proto.IndexMapping proto) {
        final double gamma = proto.getGamma();
        final double indexOffset = proto.getIndexOffset();
        switch (proto.getInterpolation()) {
            case NONE:
                return new LogarithmicMapping(gamma, indexOffset);
            case LINEAR:
                return new LinearlyInterpolatedMapping(gamma, indexOffset);
            case QUADRATIC:
                return new QuadraticallyInterpolatedMapping(gamma, indexOffset);
            case CUBIC:
                return new CubicallyInterpolatedMapping(gamma, indexOffset);
            default:
                throw new IllegalArgumentException("unrecognized interpolation");
        }
    }
}
