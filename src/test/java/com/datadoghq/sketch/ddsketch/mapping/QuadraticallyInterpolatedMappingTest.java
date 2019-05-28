package com.datadoghq.sketch.ddsketch.mapping;

class QuadraticallyInterpolatedMappingTest extends IndexMappingTest {

    @Override
    IndexMapping getMapping(double relativeAccuracy) {
        return new QuadraticallyInterpolatedMapping(relativeAccuracy);
    }
}
