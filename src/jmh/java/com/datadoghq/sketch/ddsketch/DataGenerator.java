package com.datadoghq.sketch.ddsketch;

public enum DataGenerator implements Distribution {
    POISSON(Distributions.POISSON.of(0.1)),
    COMPOSITE_POISSON_EXTREME(Distributions.POISSON.of(0.001)
            .composeWith(Distributions.POISSON.of(0.999))),
    TRIMODAL_NORMAL(Distributions.NORMAL.of(100, 10)
            .composeWith(0.33, Distributions.NORMAL.of(1000, 100)
            .composeWith(0.5, Distributions.NORMAL.of(10000, 1000))));


    private final Distribution distribution;

    DataGenerator(Distribution distribution) {
        this.distribution = distribution;
    }

    @Override
    public double nextValue() {
        return distribution.nextValue();
    }

    @Override
    public String toString() {
        return distribution.toString();
    }
}
