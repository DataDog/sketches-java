package com.datadoghq.sketch.ddsketch.mapping;

class LogarithmicMappingTest extends IndexMappingTest {

    @Override
    IndexMapping getMapping(double relativeAccuracy) {
        return new LogarithmicMapping(relativeAccuracy);
    }
}
