/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import static org.assertj.core.api.Assertions.*;

abstract class InputTest {
  //
  abstract Input input(byte... bytes);
  //
  //    private static void assertEOF(Input input) {
  //        assertThatExceptionOfType(EOFException.class).isThrownBy(input::readByte);
  //    }
  //
  ////    @ParameterizedTest
  ////    @MethodSource
  ////    void testReadVarLong(long expected, byte[] bytes) {
  ////        final Input input = input(bytes);
  ////        try {
  ////            assertThat(input.readVarLong()).isEqualTo(expected);
  ////        } catch (IOException e) {
  ////            fail("exception thrown", e);
  ////        }
  ////        assertEOF(input);
  ////    }
  //
  //    static Stream<Arguments> testReadVarLong() {
  //        return Stream.of(
  //            arguments(0L, new byte[]{0}),
  //            arguments(1L, new byte[]{(byte) 1}),
  //            arguments(127L, new byte[]{0x7F}),
  //            arguments(128L, new byte[]{(byte) 0x80, (byte) 0x01}),
  //            arguments(129L, new byte[]{(byte) 0x81, (byte) 0x01}),
  //            arguments(256L, new byte[]{(byte) 0x80, (byte) 0x02}),
  //            arguments(0x8000000000000000L, new byte[]{(byte) 0x80, (byte) 0x80, (byte)0x80,
  // (byte)0x80, (byte)0x80, (byte)0x80, (byte)0x80, (byte)0x80, (byte)0x80, (byte) 0x01}),
  //            arguments(-1L, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte)0xFF, (byte)0xFF,
  // (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte) 0x01})
  //        );
  //    }
  //
  //    @ParameterizedTest
  //    @MethodSource
  //    void testReadCorruptedVarLong(Class<? extends IOException> expectedException, byte[] bytes)
  // {
  //        final Input input = input(bytes);
  //        assertThatExceptionOfType(expectedException).isThrownBy(input::readVarLong);
  //        assertEOF(input);
  //    }
  //
  //    static Stream<Arguments> testReadCorruptedVarLong() {
  //        return Stream.of(
  //            arguments(EOFException.class, new byte[]{(byte) 0x80}),
  //            arguments(EOFException.class, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte)0xFF,
  // (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF}),
  //            arguments(CorruptedInputException.class, new byte[]{(byte) 0xFF, (byte) 0xFF,
  // (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
  // (byte)0x02})
  //        );
  //    }

  //    @ParameterizedTest
  //    @MethodSource("com.datadoghq.sketch.ddsketch.encoding.TestParameters#varDouble")
  //    void testReadVarDouble(double value, byte[] bytes) {
  //        test(value, bytes, Input::readVarDouble);
  //    }
  //
  //    private <T> void test(T value, byte[] bytes, ReadMethod<T> readMethod) {
  //        final Input input = input(bytes);
  //        try {
  //            assertThat(readMethod.read(input)).isEqualTo(value);
  //        } catch (IOException e) {
  //            fail("exception thrown", e);
  //        }
  //        assertEOF(input);
  //
  //    }
  //
  //    private interface ReadMethod<T> {
  //        T read(Input input) throws IOException;
  //    }
}
