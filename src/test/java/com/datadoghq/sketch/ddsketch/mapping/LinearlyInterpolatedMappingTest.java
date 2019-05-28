package com.datadoghq.sketch.ddsketch.mapping;

class LinearlyInterpolatedMappingTest extends IndexMappingTest {

    @Override
    IndexMapping getMapping(double relativeAccuracy) {
        return new LinearlyInterpolatedMapping(relativeAccuracy);
    }
}
