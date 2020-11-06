package com.datadoghq.sketch.ddsketch;

@FunctionalInterface
public interface Distribution {
    double nextValue();

    default Distribution composeWith(Distribution other) {
        return composeWith(0.5, other);
    }

    default Distribution composeWith(double weight, Distribution other) {
        return new CompositeDistribution(weight,this, other);
    }
}
