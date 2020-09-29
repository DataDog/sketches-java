/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

abstract class DDSketchTest extends QuantileSketchTest<DDSketch> {

    abstract double relativeAccuracy();

    IndexMapping mapping() {
        return new LogarithmicMapping(relativeAccuracy());
    }

    Supplier<Store> storeSupplier() {
        return UnboundedSizeDenseStore::new;
    }


    @Override
    public DDSketch newSketch() {
        return new DDSketch(mapping(), storeSupplier());
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

        if (actualQuantileValue < minExpected - AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR ||
            actualQuantileValue > maxExpected + AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR) {
            fail();
        }
    }

    @Test
    @Override
    protected void throwsExceptionWhenExpected() {

        super.throwsExceptionWhenExpected();

        final DDSketch sketch = newSketch();

        assertThrows(
            IllegalArgumentException.class,
            () -> sketch.accept(-1)
        );
    }

    @Override
    protected void test(boolean merged, double[] values, DDSketch sketch) {
        assertEncodes(merged, values, sketch);
        testProtoRoundTrip(merged, values, sketch);
    }

    void testProtoRoundTrip(boolean merged, double[] values, DDSketch sketch) {
        assertEncodes(merged, values, DDSketch.fromProto(storeSupplier(), sketch.toProto()));
    }

    @Test
    void testNegativeValueThrowsIllegalArgumentException() {
        final SignedDDSketch sketch = new SignedDDSketch(mapping(), storeSupplier());
        sketch.accept(-1);
        final com.datadoghq.sketch.ddsketch.proto.DDSketch proto = sketch.toProto();
        assertThrows(IllegalArgumentException.class, () -> DDSketch.fromProto(storeSupplier(), proto));
    }

    static class DDSketchTest1 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-1;
        }
    }

    static class DDSketchTest2 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-2;
        }
    }

    static class DDSketchTest3 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-3;
        }
    }
}
