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
