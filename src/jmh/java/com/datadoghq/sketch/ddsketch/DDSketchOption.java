package com.datadoghq.sketch.ddsketch;

import java.util.function.DoubleFunction;

public enum DDSketchOption {
    FAST(DDSketch::fast),
    MEMORY_OPTIMAL(DDSketch::memoryOptimal),
    BALANCED(DDSketch::balanced);

    private final DoubleFunction<DDSketch> creator;

    DDSketchOption(DoubleFunction<DDSketch> creator) {
        this.creator = creator;
    }

    public DDSketch create(double relativeAccuracy) {
        return creator.apply(relativeAccuracy);
    }
}
