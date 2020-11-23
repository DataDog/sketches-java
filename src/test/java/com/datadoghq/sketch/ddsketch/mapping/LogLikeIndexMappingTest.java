/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import com.datadoghq.sketch.ddsketch.Serializer;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    @Override
    void testProtoRoundTrip() {
        final LogLikeIndexMapping mapping = getMapping(1e-2);
        assertSameAfterProtoRoundTrip(mapping, IndexMappingProtoBinding.fromProto(IndexMappingProtoBinding.toProto(mapping)));
        Serializer serializer = new Serializer(mapping.serializedSize());
        mapping.serialize(serializer);
        try {
            assertSameAfterProtoRoundTrip(mapping, IndexMappingProtoBinding.fromProto(com.datadoghq.sketch.ddsketch.proto.IndexMapping.parseFrom(serializer.getBuffer())));
        } catch (InvalidProtocolBufferException e) {
            fail(e);
        }
    }

    private void assertSameAfterProtoRoundTrip(LogLikeIndexMapping mapping, IndexMapping roundTripMapping) {
        assertEquals(mapping.getClass(), roundTripMapping.getClass());
        assertEquals(mapping.relativeAccuracy(), roundTripMapping.relativeAccuracy(),
                AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        assertEquals(mapping.value(0), roundTripMapping.value(0),
                AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
    }

    private void testOffset(LogLikeIndexMapping mapping, double indexOffset) {
        final double indexOf1 = mapping.index(1);
        // If 1 is on a bucket boundary, its associated index can be either of the ones of the previous and the next
        // buckets.
        assertTrue(Math.ceil(indexOffset) - 1 <= indexOf1);
        assertTrue(indexOf1 <= Math.floor(indexOffset));
    }
}
