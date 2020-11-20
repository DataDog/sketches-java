package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.PaginatedStore;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.util.function.DoubleFunction;

public enum DDSketchOption {
    FAST(relativeAccuracy -> new DDSketch(new BitwiseLinearlyInterpolatedMapping(relativeAccuracy), UnboundedSizeDenseStore::new)),
    MEMORY_OPTIMAL(DDSketches::logarithmicUnboundedDense),
    BALANCED(DDSketches::unboundedDense),
    PAGINATED(relativeAccuracy -> new DDSketch(new BitwiseLinearlyInterpolatedMapping(relativeAccuracy), PaginatedStore::new));

    private final DoubleFunction<DDSketch> creator;

    DDSketchOption(DoubleFunction<DDSketch> creator) {
        this.creator = creator;
    }

    public DDSketch create(double relativeAccuracy) {
        return creator.apply(relativeAccuracy);
    }
}
