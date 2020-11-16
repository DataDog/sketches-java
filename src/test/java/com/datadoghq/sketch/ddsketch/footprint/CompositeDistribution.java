package com.datadoghq.sketch.ddsketch.footprint;

import java.util.concurrent.ThreadLocalRandom;

final class CompositeDistribution implements Distribution {
    private final Distribution first;
    private final Distribution second;

    CompositeDistribution(Distribution first, Distribution second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "/" + second;
    }

    @Override
    public double nextValue() {
        return ThreadLocalRandom.current().nextBoolean()
                ? first.nextValue()
                : second.nextValue();
    }
}
