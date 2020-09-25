/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

class CubicallyInterpolatedMappingTest extends LogLikeIndexMappingTest {

    @Override
    CubicallyInterpolatedMapping getMapping(double relativeAccuracy) {
        return new CubicallyInterpolatedMapping(relativeAccuracy);
    }

    @Override
    CubicallyInterpolatedMapping getMapping(double gamma, double indexOffset) {
        return new CubicallyInterpolatedMapping(gamma, indexOffset);
    }
}
