/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

abstract class OutputTest<O> {

  abstract O output();

  abstract byte[] bytes(O output);

  @ParameterizedTest
  @MethodSource("com.datadoghq.sketch.ddsketch.encoding.TestParameters#varDouble")
  void testWriteVarDouble(double value, byte[] expected) {}

  //    private <T> void test(T value, byte[] bytes, WriteMethod<T> writeMethod) {
  //        final O output = output();
  //        try {
  //            writeMethod.write(output, value);
  //        } catch (IOException e) {
  //            fail("exception thrown", e);
  //        }
  //        output.
  //    }

  private abstract class WriteMethod<T> {
    abstract void write(O output, T value) throws IOException;
  }
}
