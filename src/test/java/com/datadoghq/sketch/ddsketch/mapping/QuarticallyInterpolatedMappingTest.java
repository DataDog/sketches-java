/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

class QuarticallyInterpolatedMappingTest extends LogLikeIndexMappingTest {

  @Override
  QuarticallyInterpolatedMapping getMapping(double relativeAccuracy) {
    return new QuarticallyInterpolatedMapping(relativeAccuracy);
  }

  @Override
  QuarticallyInterpolatedMapping getMapping(double gamma, double indexOffset) {
    return new QuarticallyInterpolatedMapping(gamma, indexOffset);
  }
}
