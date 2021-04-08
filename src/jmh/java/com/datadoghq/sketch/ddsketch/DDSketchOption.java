/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

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
