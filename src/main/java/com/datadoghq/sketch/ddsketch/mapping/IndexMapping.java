package com.datadoghq.sketch.ddsketch.mapping;

/**
 * A mapping between {@code double} values and {@code integer} values that imposes relative guarantees on the composition of {@link #value} and {@link #index}. In other words, for any value {@code v} between {@link #minIndexableValue()} and {@link #maxIndexableValue()}, {@code value(index(v))} is close to {@code v} with a relative error that is less than {@link #relativeAccuracy()}.
 */
public interface IndexMapping {

    int index(double value);

    double value(int index);

    double relativeAccuracy();

    double minIndexableValue();

    double maxIndexableValue();
}
