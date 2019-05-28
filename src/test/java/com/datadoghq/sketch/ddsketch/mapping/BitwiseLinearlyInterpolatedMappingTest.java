package com.datadoghq.sketch.ddsketch.mapping;

class BitwiseLinearlyInterpolatedMappingTest extends IndexMappingTest {

    @Override
    IndexMapping getMapping(double relativeAccuracy) {
        return new BitwiseLinearlyInterpolatedMapping((relativeAccuracy));
    }
}
