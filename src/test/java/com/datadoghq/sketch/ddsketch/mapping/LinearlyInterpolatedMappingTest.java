/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LinearlyInterpolatedMappingTest extends LogLikeIndexMappingTest {

  @Override
  LinearlyInterpolatedMapping getMapping(double relativeAccuracy) {
    return new LinearlyInterpolatedMapping(relativeAccuracy);
  }

  @Override
  LinearlyInterpolatedMapping getMapping(double gamma, double indexOffset) {
    return new LinearlyInterpolatedMapping(gamma, indexOffset);
  }

  @Test
  void testIndexOffset() {
    // This is inconsistent with other mappings but this has to be maintained to ensure backward
    // compatibility. Other mappings, when constructed from the relative accuracy, map 1 to 0.
    assertThat(new LinearlyInterpolatedMapping(1e-1).index(1)).isEqualTo(4);
    assertThat(new LinearlyInterpolatedMapping(1e-2).index(1)).isEqualTo(49);
  }
}
