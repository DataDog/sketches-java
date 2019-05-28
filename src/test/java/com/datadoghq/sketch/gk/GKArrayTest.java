package com.datadoghq.sketch.gk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datadoghq.sketch.util.QuantileSketchTest;
import com.datadoghq.sketch.util.accuracy.RankAccuracyTester;
import java.util.NoSuchElementException;

public class GKArrayTest extends QuantileSketchTest<GKArray> {

    @Override
    public GKArray newSketch() {
        return new GKArray(1e-2);
    }

    @Override
    public void addToSketch(GKArray sketch, double value) {
        sketch.add(value);
    }

    @Override
    public void merge(GKArray sketch, GKArray other) {
        sketch.mergeWith(other);
    }

    private void assertAccurate(GKArray sketch, double[] values, double maxRankError) {

        assertEquals(values.length, sketch.getTotalCount());

        if (sketch.isEmpty()) {

            assertThrows(NoSuchElementException.class, sketch::getMinValue);
            assertThrows(NoSuchElementException.class, sketch::getMaxValue);
            assertThrows(NoSuchElementException.class, () -> sketch.getValueAtQuantile(0));
            assertThrows(NoSuchElementException.class, () -> sketch.getValueAtQuantile(1));

        } else {

            final RankAccuracyTester accuracyTester = new RankAccuracyTester(values);

            accuracyTester.assertMinExact(sketch.getMinValue());
            accuracyTester.assertMaxExact(sketch.getMaxValue());
            accuracyTester.assertMinExact(sketch::getValueAtQuantile);
            accuracyTester.assertMaxExact(sketch::getValueAtQuantile);

            accuracyTester.assertAccurate(maxRankError, sketch::getValueAtQuantile);

        }
    }

    @Override
    public void assertAddingAccurate(GKArray sketch, double[] values) {
        assertAccurate(sketch, values, sketch.getRankAccuracy());
    }

    @Override
    public void assertMergingAccurate(GKArray sketch, double[] values) {
        assertAccurate(sketch, values, 2 * sketch.getRankAccuracy());
    }
}
