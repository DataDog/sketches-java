/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import org.junit.jupiter.api.Test;

class BitwiseLinearlyInterpolatedMappingTest extends IndexMappingTest {

  @Override
  BitwiseLinearlyInterpolatedMapping getMapping(double relativeAccuracy) {
    return new BitwiseLinearlyInterpolatedMapping(relativeAccuracy);
  }

  @Test
  @Override
  void testProtoRoundTrip() {
    final BitwiseLinearlyInterpolatedMapping mapping = getMapping(1e-2);
    final IndexMapping roundTripMapping =
        IndexMappingProtoBinding.fromProto(IndexMappingProtoBinding.toProto(mapping));
    assertEquals(LinearlyInterpolatedMapping.class, roundTripMapping.getClass());
    assertEquals(
        mapping.relativeAccuracy(),
        roundTripMapping.relativeAccuracy(),
        AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
    assertEquals(
        mapping.value(0),
        roundTripMapping.value(0),
        AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
  }
}
