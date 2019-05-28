package com.datadoghq.sketch.benchmark;

import java.util.Arrays;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;

public interface QuantileSketch<S> extends DoubleConsumer, DoubleUnaryOperator, Supplier<S> {

    default void acceptAll(double... values) {
        Arrays.stream(values).forEach(this);
    }

    long getMemorySizeEstimate();

    void mergeWith(QuantileSketch<? extends S> otherSketch);
}
