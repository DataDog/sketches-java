package com.datadoghq.sketch.ddsketch.mapping;

import java.util.Objects;

/**
 * A base class for mappings that are derived from a function that approximates the logarithm, namely {@link #log}.
 * <p>
 * That function is scaled depending on the targeted relative accuracy, the base of the logarithm that {@link #log}
 * approximates and how well it geometrically pulls apart values from one another, that is to say, the infimum of
 * |l(x)-l(y)|/|x-y| where x ≠ y and l = log∘exp (log being {@link #log}).
 */
abstract class LogLikeIndexMapping implements IndexMapping {

    private final double relativeAccuracy;
    private final double multiplier;

    LogLikeIndexMapping(double relativeAccuracy) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        this.relativeAccuracy = relativeAccuracy;
        this.multiplier = correctingFactor() * Math.log(base())
            / (Math.log((1 + relativeAccuracy) / (1 - relativeAccuracy)));
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
     * as well as the logarithm
     */
    abstract double correctingFactor();

    @Override
    public int index(double value) {
        final double index = log(value) * multiplier;
        return index >= 0 ? (int) index : (int) index - 1; // faster than Math::floor
    }

    @Override
    public double value(int index) {
        return logInverse((double) index / multiplier) * (1 + relativeAccuracy);
    }

    @Override
    public double relativeAccuracy() {
        return relativeAccuracy;
    }

    @Override
    public double minIndexableValue() {
        return Math.max(
            Math.pow(base(), Integer.MIN_VALUE / multiplier - log(1) + 1), // so that index >= Integer.MIN_VALUE
            Double.MIN_NORMAL * (1 + relativeAccuracy) / (1 - relativeAccuracy)
        );
    }

    @Override
    public double maxIndexableValue() {
        return Math.min(
            Math.pow(base(), Integer.MAX_VALUE / multiplier - log(1) - 1), // so that index <= Integer.MAX_VALUE
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
        return Double.compare(relativeAccuracy, ((LogLikeIndexMapping) o).relativeAccuracy) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativeAccuracy);
    }
}
