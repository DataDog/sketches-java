/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

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
