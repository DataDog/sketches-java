/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;

public class VarEncodingHelper {

  private static final int MAX_VAR_LEN_64 = 9;
  private static final int VAR_DOUBLE_ROTATE_DISTANCE = 6;

  public static void encodeUnsignedVarLong(final Output output, long value) throws IOException {
    for (int i = 0; i < MAX_VAR_LEN_64 - 1; i++) {
      if (value >= 0 && value < 0x80L) {
        break;
      }
      output.writeByte((byte) (value | 0x80L));
      value >>>= 7;
    }
    output.writeByte((byte) value);
  }

  public static long decodeUnsignedVarLong(final Input input) throws IOException {
    long value = 0;
    for (int shift = 0; ; shift += 7) {
      final byte next = input.readByte();
      if (next >= 0 || shift == 7 * 8) {
        return value | ((long) next << shift);
      }
      value |= ((long) next & 0x7FL) << shift;
    }
  }

  public static void encodeSignedVarLong(final Output output, final long value) throws IOException {
    encodeUnsignedVarLong(output, (value >> (64 - 1) ^ (value << 1)));
  }

  public static long decodeSignedVarLong(final Input input) throws IOException {
    final long value = decodeUnsignedVarLong(input);
    return (value >>> 1) ^ -(value & 1);
  }

  public static void encodeVarDouble(final Output output, final double value) throws IOException {
    long bits =
        Long.rotateLeft(
            Double.doubleToRawLongBits(value + 1) - Double.doubleToRawLongBits(1),
            VAR_DOUBLE_ROTATE_DISTANCE);
    for (int i = 0; i < MAX_VAR_LEN_64 - 1; i++) {
      final byte next = (byte) (bits >>> (8 * 8 - 7));
      bits <<= 7;
      if (bits == 0) {
        output.writeByte(next);
        return;
      }
      output.writeByte((byte) (next | 0x80L));
    }
    output.writeByte((byte) (bits >>> (8 * 7)));
  }

  public static double decodeVarDouble(final Input input) throws IOException {
    long bits = 0;
    for (int shift = 8 * 8 - 7; ; shift -= 7) {
      final byte next = input.readByte();
      if (shift == 1) {
        bits |= Byte.toUnsignedLong(next);
        break;
      }
      if (next >= 0) {
        bits |= (long) next << shift;
        break;
      }
      bits |= ((long) next & 0x7FL) << shift;
    }
    return Double.longBitsToDouble(
            Long.rotateRight(bits, VAR_DOUBLE_ROTATE_DISTANCE) + Double.doubleToRawLongBits(1))
        - 1;
  }
}
