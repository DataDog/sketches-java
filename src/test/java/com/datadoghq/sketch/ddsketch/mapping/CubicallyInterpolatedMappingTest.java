package com.datadoghq.sketch.ddsketch.mapping;

public class CubicallyInterpolatedMappingTest extends IndexMappingTest {

    @Override
    IndexMapping getMapping(double relativeAccuracy) {
        return new CubicallyInterpolatedMapping(relativeAccuracy);
    }
}
