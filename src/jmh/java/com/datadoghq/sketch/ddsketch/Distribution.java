/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

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
