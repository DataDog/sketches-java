package com.datadoghq.sketch.ddsketch.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class LogLikeIndexMappingTest extends IndexMappingTest {

    private static final double[] TEST_GAMMAS = new double[]{1 + 1e-6, 1.02, 1.5};
    private static final double[] TEST_INDEX_OFFSETS = new double[]{0, 1, -12.23, 7768.3};

    abstract LogLikeIndexMapping getMapping(double relativeAccuracy);

    abstract LogLikeIndexMapping getMapping(double gamma, double indexOffset);

    @Test
    @Override
    void testAccuracy() {
        super.testAccuracy();

        for (final double gamma : TEST_GAMMAS) {
            for (final double indexOffset : TEST_INDEX_OFFSETS) {
                final LogLikeIndexMapping mapping = getMapping(gamma, indexOffset);
                testAccuracy(mapping, mapping.relativeAccuracy());
            }
        }
    }

    @Test
    void testOffset() {
        for (final double gamma : TEST_GAMMAS) {
            for (final double indexOffset : TEST_INDEX_OFFSETS) {
                testOffset(getMapping(gamma, indexOffset), indexOffset);
            }
        }
    }

    private void testOffset(LogLikeIndexMapping mapping, double indexOffset) {
        final double indexOf1 = mapping.index(1);
        // If 1 is on a bucket boundary, its associated index can be either of the ones of the previous and the next
        // buckets.
        assertTrue(Math.ceil(indexOffset) - 1 <= indexOf1);
        assertTrue(indexOf1 <= Math.floor(indexOffset));
    }
}