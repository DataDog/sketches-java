package com.datadoghq.sketch.ddsketch.footprint;

@FunctionalInterface
public interface Distribution {
    double nextValue();

    default Distribution composeWith(Distribution other) {
        return new CompositeDistribution(this, other);
    }
}
