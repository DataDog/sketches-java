/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.ddsketch.encoding.ByteArrayInput;
import com.datadoghq.sketch.ddsketch.encoding.Flag;
import com.datadoghq.sketch.ddsketch.encoding.GrowingByteArrayOutput;
import com.datadoghq.sketch.ddsketch.encoding.IndexMappingLayout;
import com.datadoghq.sketch.ddsketch.encoding.Input;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import java.io.IOException;
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

  @Test
  @Override
  void testEncodeDecode() {
    final BitwiseLinearlyInterpolatedMapping mapping = getMapping(1e-2);
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      mapping.encode(output);
    } catch (IOException e) {
      fail(e);
    }

    final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
    final IndexMapping decoded;
    try {
      final Flag flag = Flag.decode(input);
      decoded = IndexMapping.decode(input, IndexMappingLayout.ofFlag(flag));
    } catch (IOException e) {
      fail(e);
      return;
    }
    // TODO: decoded could be a BitwiseLinearlyInterpolatedMapping
    assertEquals(LinearlyInterpolatedMapping.class, decoded.getClass());
    assertEquals(
        mapping.relativeAccuracy(),
        decoded.relativeAccuracy(),
        AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
    assertEquals(
        mapping.value(0), decoded.value(0), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
  }
}
