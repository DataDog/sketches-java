/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class TestParameters {

  static Stream<Arguments> varDouble() {
    return Stream.of(arguments(0.0, new byte[] {0x00}), arguments(1.0, new byte[] {0x04}));
  }
}
