package com.datadoghq.sketch.ddsketch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datadoghq.sketch.util.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import com.datadoghq.sketch.util.accuracy.RelativeAccuracyTester;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

class DDSketchTest extends QuantileSketchTest<DDSketch> {

    private final IndexMapping mapping = new LogarithmicMapping(1e-2);

    @Override
    public DDSketch newSketch() {
        return new DDSketch(mapping, UnboundedSizeDenseStore::new);
    }

    @Override
    public void addToSketch(DDSketch sketch, double value) {
        sketch.add(value);
    }

    @Override
    public void merge(DDSketch sketch, DDSketch other) {
        sketch.mergeWith(other);
    }

    private void assertAccurate(DDSketch sketch, double[] values) {

        assertEquals(values.length, sketch.getTotalCount());

        if (values.length == 0) {
            assertThrows(NoSuchElementException.class, sketch::getMin);
            assertThrows(NoSuchElementException.class, sketch::getMax);
            assertThrows(NoSuchElementException.class, () -> sketch.getValueAtQuantile(0));
            assertThrows(NoSuchElementException.class, () -> sketch.getValueAtQuantile(1));
        } else {
            final RelativeAccuracyTester relativeAccuracyTester = new RelativeAccuracyTester(values);
            relativeAccuracyTester.assertAccurate(mapping.relativeAccuracy(), sketch::getValueAtQuantile);
        }
    }

    @Override
    public void assertAddingAccurate(DDSketch sketch, double[] values) {
        assertAccurate(sketch, values);
    }

    @Override
    public void assertMergingAccurate(DDSketch sketch, double[] values) {
        assertAccurate(sketch, values);
    }

    @Override
    public double randomValue() {
        return ThreadLocalRandom.current().nextDouble(mapping.minIndexableValue(), 1e8);
    }
}
