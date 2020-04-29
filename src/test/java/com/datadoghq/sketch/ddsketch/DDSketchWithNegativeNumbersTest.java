/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import java.util.function.Supplier;

abstract class DDSketchWithNegativeNumbersTest extends QuantileSketchTest<DDSketchWithNegativeNumbers> {

    abstract double relativeAccuracy();

    IndexMapping mapping() {
        return new LogarithmicMapping(relativeAccuracy());
    }

    Supplier<Store> storeSupplier() {
        return UnboundedSizeDenseStore::new;
    }


    @Override
    public DDSketchWithNegativeNumbers newSketch() {
        return new DDSketchWithNegativeNumbers(mapping(), storeSupplier());
    }

    @Override
    protected void assertAccurate(boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {

        if (sortedValues[0] < 0) {
            throw new IllegalArgumentException();
        }
        if (actualQuantileValue < 0) {
            fail();
        }

        final double lowerQuantileValue = sortedValues[(int) Math.floor(quantile * (sortedValues.length - 1))];
        final double upperQuantileValue = sortedValues[(int) Math.ceil(quantile * (sortedValues.length - 1))];

        final double minExpected = lowerQuantileValue * (1 - relativeAccuracy());
        final double maxExpected = upperQuantileValue * (1 + relativeAccuracy());

        if (actualQuantileValue < minExpected || actualQuantileValue > maxExpected) {
            fail();
        }
    }

    static class DDSketchWithNegativeNumbersTest1 extends DDSketchWithNegativeNumbersTest {

        @Override
        double relativeAccuracy() {
            return 1e-1;
        }
    }

    static class DDSketchWithNegativeNumbersTest2 extends DDSketchWithNegativeNumbersTest {

        @Override
        double relativeAccuracy() {
            return 1e-2;
        }
    }

    static class DDSketchWithNegativeNumbersTest3 extends DDSketchWithNegativeNumbersTest {

        @Override
        double relativeAccuracy() {
            return 1e-3;
        }
    }

}
