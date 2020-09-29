/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BitwiseLinearlyInterpolatedMappingTest extends IndexMappingTest {

    @Override
    BitwiseLinearlyInterpolatedMapping getMapping(double relativeAccuracy) {
        return new BitwiseLinearlyInterpolatedMapping(relativeAccuracy);
    }

    @Test
    @Override
    void testProtoRoundTrip() {
        final BitwiseLinearlyInterpolatedMapping mapping = getMapping(1e-2);
        final IndexMapping roundTripMapping = IndexMapping.fromProto(mapping.toProto());
        final IndexMapping expectedMapping = new LinearlyInterpolatedMapping(mapping.relativeAccuracy());
        assertEquals(expectedMapping.getClass(), roundTripMapping.getClass());
        assertEquals(expectedMapping.relativeAccuracy(), roundTripMapping.relativeAccuracy(),
            AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        assertEquals(expectedMapping.value(0), roundTripMapping.value(0),
            AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
    }
}
