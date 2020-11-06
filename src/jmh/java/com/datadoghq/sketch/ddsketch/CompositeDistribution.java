package com.datadoghq.sketch.ddsketch;

import java.util.concurrent.ThreadLocalRandom;

final class CompositeDistribution implements Distribution {

    private final double weight;
    private final Distribution first;
    private final Distribution second;

    CompositeDistribution(double weight, Distribution first, Distribution second) {
        this.weight = weight;
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "/" + second;
    }

    @Override
    public double nextValue() {
        return ThreadLocalRandom.current().nextDouble() < weight
                ? first.nextValue()
                : second.nextValue();
    }
}
