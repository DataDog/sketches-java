/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;

public final class VarEncodingHelper {

  private static final int MAX_VAR_LEN_64 = 9;
  private static final int VAR_DOUBLE_ROTATE_DISTANCE = 6;

  private VarEncodingHelper() {}

  /**
   * {@code encodeUnsignedVarLong} serializes {@code long} values 7 bits at a time, starting with
   * the least significant bits. The most significant bit in each output byte is the continuation
   * bit and indicates whether there are additional non-zero bits encoded in following bytes. There
   * are at most 9 output bytes and the last one does not have a continuation bit, allowing for it
   * to encode 8 bits (\(8*7+8 = 64\)).
   *
   * @param output what to write to
   * @param value the value to encode
   * @throws IOException if an {@link IOException} is thrown while writing to {@code output}
   */
  public static void encodeUnsignedVarLong(final Output output, long value) throws IOException {
    final int length = (63 - Long.numberOfLeadingZeros(value)) / 7;
    for (int i = 0; i < length && i < 8; i++) {
      output.writeByte((byte) (value | 0x80L));
      value >>>= 7;
    }
    output.writeByte((byte) value);
  }

  /**
   * {@code decodeUnsignedVarLong} deserializes {@code long} values that have been encoded using
   * {@link #encodeUnsignedVarLong(Output, long)}.
   *
   * @param input what to read from
   * @return the decoded value
   * @throws IOException if an {@link IOException} is thrown while reading from {@code input}
   */
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

  /**
   * {@code encodeSignedVarLong} serializes {@code long} values using zig-zag encoding, which
   * ensures small-scale integers are turned into integers that have leading zeros, whether they are
   * positive or negative, hence allows for space-efficient encoding of those values.
   *
   * @param output what to write to
   * @param value the value to encode
   * @throws IOException if an {@link IOException} is thrown while writing to {@code output}
   */
  public static void encodeSignedVarLong(final Output output, final long value) throws IOException {
    encodeUnsignedVarLong(output, (value >> (64 - 1) ^ (value << 1)));
  }

  /**
   * {@code decodeSignedVarLong} deserializes {@code long} values that have been encoded using
   * {@link #encodeSignedVarLong(Output, long)}.
   *
   * @param input what to read from
   * @return the decoded value
   * @throws IOException if an {@link IOException} is thrown while reading from {@code input}
   */
  public static long decodeSignedVarLong(final Input input) throws IOException {
    final long value = decodeUnsignedVarLong(input);
    return (value >>> 1) ^ -(value & 1);
  }

  /**
   * {@code encodeVarDouble} serializes {@code double} values using a method that is similar to the
   * varint encoding and that is space-efficient for non-negative integer values. The output takes
   * at most 9 bytes.
   *
   * <p>Input values are first shifted as floating-point values ({@code +1}), then transmuted to
   * integer values, then shifted again as integer values ({@code -Double.doubleToRawLongBits(1)}).
   * That is in order to minimize the number of non-zero bits when dealing with non-negative integer
   * values.
   *
   * <p>After that transformation, any input integer value no greater than \(2^{53}\) (the largest
   * integer value that can be encoded exactly as a 64-bit floating-point value) will have at least
   * 6 leading zero bits. By rotating bits to the left, those bits end up at the right of the binary
   * representation.
   *
   * <p>The resulting bits are then encoded similarly to the varint method, but starting with the
   * most significant bits.
   *
   * @param output what to write to
   * @param value the value to encode
   * @throws IOException if an {@link IOException} is thrown while writing to {@code output}
   */
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

  /**
   * {@code decodeSignedVarLong} deserializes {@code double} values that have been encoded using
   * {@link #encodeVarDouble(Output, double)}.
   *
   * @param input what to read from
   * @return the decoded value
   * @throws IOException if an {@link IOException} is thrown while reading from {@code input}
   */
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
