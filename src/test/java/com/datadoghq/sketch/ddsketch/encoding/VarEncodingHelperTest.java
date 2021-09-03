/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VarEncodingHelperTest {

  @ParameterizedTest
  @MethodSource("unsignedVarLongs")
  void testEncodeUnsignedVarLong(long value, byte[] bytes) {
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      VarEncodingHelper.encodeUnsignedVarLong(output, value);
    } catch (IOException e) {
      fail(e.toString());
      return;
    }
    assertThat(output.trimmedCopy()).isEqualTo(bytes);
  }

  @ParameterizedTest
  @MethodSource("unsignedVarLongs")
  void testDecodeUnsignedVarLong(long value, byte[] bytes) {
    final Input input = ByteArrayInput.wrap(bytes);
    final long decoded;
    try {
      decoded = VarEncodingHelper.decodeUnsignedVarLong(input);
    } catch (IOException e) {
      fail(e.toString());
      return;
    }
    assertThat(decoded).isEqualTo(value);
  }

  @ParameterizedTest
  @MethodSource("unsignedVarLongs")
  void testUnsignedVarLongEncodedLength(long value, byte[] bytes) {
    assertThat((int) VarEncodingHelper.unsignedVarLongEncodedLength(value)).isEqualTo(bytes.length);
  }

  static Stream<Arguments> unsignedVarLongs() {
    return Stream.of(
        arguments(0L, new byte[] {0x00}),
        arguments(1, new byte[] {0x01}),
        arguments(127, new byte[] {0x7F}),
        arguments(128, new byte[] {(byte) 0x80, 0x01}),
        arguments(129, new byte[] {(byte) 0x81, 0x01}),
        arguments(255, new byte[] {(byte) 0xFF, 0x01}),
        arguments(256, new byte[] {(byte) 0x80, 0x02}),
        arguments(16383, new byte[] {(byte) 0xFF, 0x7F}),
        arguments(16384, new byte[] {(byte) 0x80, (byte) 0x80, 0x01}),
        arguments(16385, new byte[] {(byte) 0x81, (byte) 0x80, 0x01}),
        arguments(
            -2,
            new byte[] {
              (byte) 0xFE,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) (byte) 0xFF,
              (byte) 0xFF,
              (byte) (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            }),
        arguments(
            -1,
            new byte[] {
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            }));
  }

  @ParameterizedTest
  @MethodSource("signedVarLongs")
  void testEncodeSignedVarLong(long value, byte[] bytes) {
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      VarEncodingHelper.encodeSignedVarLong(output, value);
    } catch (IOException e) {
      fail(e.toString());
      return;
    }
    assertThat(output.trimmedCopy()).isEqualTo(bytes);
  }

  @ParameterizedTest
  @MethodSource("signedVarLongs")
  void testDecodeSignedVarLong(long value, byte[] bytes) {
    final Input input = ByteArrayInput.wrap(bytes);
    final long decoded;
    try {
      decoded = VarEncodingHelper.decodeSignedVarLong(input);
    } catch (IOException e) {
      fail(e.toString());
      return;
    }
    assertThat(decoded).isEqualTo(value);
  }

  @ParameterizedTest
  @MethodSource("signedVarLongs")
  void testSignedVarLongEncodedLength(long value, byte[] bytes) {
    assertThat((int) VarEncodingHelper.signedVarLongEncodedLength(value)).isEqualTo(bytes.length);
  }

  static Stream<Arguments> signedVarLongs() {
    return Stream.of(
        arguments(0L, new byte[] {0x00}),
        arguments(1L, new byte[] {0x02}),
        arguments(63L, new byte[] {0x7E}),
        arguments(64L, new byte[] {(byte) 0x80, 0x01}),
        arguments(65L, new byte[] {(byte) 0x82, 0x01}),
        arguments(127L, new byte[] {(byte) 0xFE, 0x01}),
        arguments(128L, new byte[] {(byte) 0x80, 0x02}),
        arguments(8191L, new byte[] {(byte) 0xFE, 0x7F}),
        arguments(8192L, new byte[] {(byte) 0x80, (byte) 0x80, 0x01}),
        arguments(8193L, new byte[] {(byte) 0x82, (byte) 0x80, 0x01}),
        arguments(
            (Long.MAX_VALUE >> 1) - 1L,
            new byte[] {
              (byte) 0xFC,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              0x7F
            }),
        arguments(
            Long.MAX_VALUE >> 1,
            new byte[] {
              (byte) 0xFE,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              0x7F
            }),
        arguments(
            (Long.MAX_VALUE >> 1) + 1L,
            new byte[] {
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80
            }),
        arguments(
            Long.MAX_VALUE - 1L,
            new byte[] {
              (byte) 0xFC,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            }),
        arguments(
            Long.MAX_VALUE,
            new byte[] {
              (byte) 0xFE,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            }),
        arguments(-1L, new byte[] {0x01}),
        arguments(-63L, new byte[] {0x7D}),
        arguments(-64L, new byte[] {0x7F}),
        arguments(-65L, new byte[] {(byte) 0x81, 0x01}),
        arguments(-127L, new byte[] {(byte) 0xFD, 0x01}),
        arguments(-128L, new byte[] {(byte) 0xFF, 0x01}),
        arguments(-8191L, new byte[] {(byte) 0xFD, 0x7F}),
        arguments(-8192L, new byte[] {(byte) 0xFF, 0x7F}),
        arguments(-8193L, new byte[] {(byte) 0x81, (byte) 0x80, 0x01}),
        arguments(
            (Long.MIN_VALUE >> 1) + 1L,
            new byte[] {
              (byte) 0xFD,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              0x7F
            }),
        arguments(
            Long.MIN_VALUE >> 1L,
            new byte[] {
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              0x7F
            }),
        arguments(
            (Long.MIN_VALUE >> 1) - 1L,
            new byte[] {
              (byte) 0x81,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80
            }),
        arguments(
            Long.MIN_VALUE + 1L,
            new byte[] {
              (byte) 0xFD,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            }),
        arguments(
            Long.MIN_VALUE,
            new byte[] {
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF
            }));
  }

  @ParameterizedTest
  @MethodSource("varDoubles")
  void testEncodeVarDouble(double value, byte[] bytes) {
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      VarEncodingHelper.encodeVarDouble(output, value);
    } catch (IOException e) {
      fail(e.toString());
      return;
    }
    assertThat(output.trimmedCopy()).isEqualTo(bytes);
  }

  @ParameterizedTest
  @MethodSource("varDoubles")
  void testDecodeVarDouble(double value, byte[] bytes) {
    final Input input = ByteArrayInput.wrap(bytes);
    final double decoded;
    try {
      decoded = VarEncodingHelper.decodeVarDouble(input);
    } catch (IOException e) {
      fail(e.toString());
      return;
    }
    assertThat(decoded).isEqualTo(value);
  }

  @ParameterizedTest
  @MethodSource("varDoubles")
  void testVarDoubleEncodedLength(double value, byte[] bytes) {
    assertThat((int) VarEncodingHelper.varDoubleEncodedLength(value)).isEqualTo(bytes.length);
  }

  static Stream<Arguments> varDoubles() {
    return Stream.of(
        arguments(0, new byte[] {0x00}),
        arguments(1.0, new byte[] {0x02}),
        arguments(2.0, new byte[] {0x03}),
        arguments(3.0, new byte[] {0x04}),
        arguments(4.0, new byte[] {(byte) 0x84, 0x40}),
        arguments(5.0, new byte[] {0x05}),
        arguments(6.0, new byte[] {(byte) 0x85, 0x40}),
        arguments(7.0, new byte[] {0x06}),
        arguments(8.0, new byte[] {(byte) 0x86, 0x20}),
        arguments(9.0, new byte[] {(byte) 0x86, 0x40}),
        arguments(
            (double) (1L << 52) - 2,
            new byte[] {
              (byte) 0xE7,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0x80
            }),
        arguments((double) (1L << 52) - 1, new byte[] {0x68}),
        arguments(
            (double) (1L << 52),
            new byte[] {
              (byte) 0xE8,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              0x40
            }),
        arguments(
            (double) (1L << 53) - 2,
            new byte[] {
              (byte) 0xE9,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xC0
            }),
        arguments((double) (1L << 53) - 1, new byte[] {0x6A}),
        arguments(
            -1.0,
            new byte[] {
              (byte) 0x82,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              0x30
            }),
        arguments(
            -0.5,
            new byte[] {
              (byte) 0xFE,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              (byte) 0x80,
              0x3F
            }));
  }
}
