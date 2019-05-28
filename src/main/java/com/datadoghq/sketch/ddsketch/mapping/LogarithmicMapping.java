package com.datadoghq.sketch.ddsketch.mapping;

import java.util.Objects;

/**
 * Memory-optimal.
 */
public class LogarithmicMapping implements IndexMapping {

    private final double relativeAccuracy;
    private final double logGamma;

    public LogarithmicMapping(double relativeAccuracy) {
        if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
            throw new IllegalArgumentException("The relative accuracy must be between 0 and 1.");
        }
        this.relativeAccuracy = relativeAccuracy;
        this.logGamma = Math.log((1 + relativeAccuracy) / (1 - relativeAccuracy));
    }

    @Override
    public int index(double value) {
        final double index = Math.log(value) / logGamma;
        return index >= 0 ? (int) index : (int) index - 1; // faster than Math.floor()
    }

    @Override
    public double value(int index) {
        return Math.exp(index * logGamma) * (1 + relativeAccuracy);
    }

    @Override
    public double relativeAccuracy() {
        return relativeAccuracy;
    }

    @Override
    public double minIndexableValue() {
        return Math.max(
                Math.exp((Integer.MIN_VALUE + 1) * logGamma), // so that index >= Integer.MIN_VALUE
                Double.MIN_NORMAL * Math.exp(logGamma) // so that Math.exp(index * logGamma) >= Double.MIN_NORMAL
        );
    }

    @Override
    public double maxIndexableValue() {
        return Math.min(
                Math.exp(Integer.MAX_VALUE * logGamma), // so that index <= Integer.MAX_VALUE
                Double.MAX_VALUE / (1 + relativeAccuracy) // so that value >= Double.MAX_VALUE
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
        return Double.compare(relativeAccuracy, ((LogarithmicMapping) o).relativeAccuracy) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativeAccuracy);
    }
}
