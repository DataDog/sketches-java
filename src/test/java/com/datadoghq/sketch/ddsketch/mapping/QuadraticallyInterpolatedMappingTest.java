/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

class QuadraticallyInterpolatedMappingTest extends LogLikeIndexMappingTest {

    @Override
    QuadraticallyInterpolatedMapping getMapping(double relativeAccuracy) {
        return new QuadraticallyInterpolatedMapping(relativeAccuracy);
    }

    @Override
    QuadraticallyInterpolatedMapping getMapping(double gamma, double indexOffset) {
        return new QuadraticallyInterpolatedMapping(gamma, indexOffset);
    }
}
