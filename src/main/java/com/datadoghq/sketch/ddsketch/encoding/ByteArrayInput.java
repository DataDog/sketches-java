/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Objects;
import sun.misc.Unsafe;

/** An implementation of {@link Input} that is backed by an array. */
public class ByteArrayInput implements Input {

  private static final Unsafe UNSAFE;

  static {
    Unsafe unsafe = null;
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = (Unsafe) f.get(null);
    } catch (IllegalAccessException | NoSuchFieldException ignored) {
    }
    UNSAFE = unsafe;
  }

  final byte[] array;
  final int endPos;
  int pos;

  private ByteArrayInput(byte[] array, int offset, int length) {
    Objects.requireNonNull(array);
    if (offset < 0 || offset + length > array.length) {
      throw new IndexOutOfBoundsException();
    }
    this.array = array;
    this.endPos = offset + length;
    this.pos = offset;
  }

  public static ByteArrayInput wrap(byte[] array, int offset, int length) {
    final ByteOrder nativeOrder = ByteOrder.nativeOrder();
    if (ByteOrder.LITTLE_ENDIAN.equals(nativeOrder)) {
      return new LittleEndianNativeOrderByteArrayInput(array, offset, length);
    } else if (ByteOrder.BIG_ENDIAN.equals(nativeOrder)) {
      return new BigEndianNativeOrderByteArrayInput(array, offset, length);
    } else {
      return new ByteArrayInput(array, offset, length);
    }
  }

  public static ByteArrayInput wrap(byte[] array) {
    return wrap(array, 0, array.length);
  }

  @Override
  public final boolean hasRemaining() {
    return pos < endPos;
  }

  @Override
  public final byte readByte() throws EOFException {
    if (pos >= endPos) {
      throw new EOFException();
    }
    return array[pos++];
  }

  @Override
  public long readLongLE() throws IOException {
    if (pos > endPos - 8) {
      throw new EOFException();
    }
    long value = 0;
    value |= Byte.toUnsignedLong(array[pos]);
    value |= Byte.toUnsignedLong(array[pos + 1]) << 8;
    value |= Byte.toUnsignedLong(array[pos + 2]) << 16;
    value |= Byte.toUnsignedLong(array[pos + 3]) << 24;
    value |= Byte.toUnsignedLong(array[pos + 4]) << 32;
    value |= Byte.toUnsignedLong(array[pos + 5]) << 40;
    value |= Byte.toUnsignedLong(array[pos + 6]) << 48;
    value |= Byte.toUnsignedLong(array[pos + 7]) << 56;
    pos += 8;
    return value;
  }

  private static class BigEndianNativeOrderByteArrayInput extends ByteArrayInput {

    public BigEndianNativeOrderByteArrayInput(byte[] array, int offset, int length) {
      super(array, offset, length);
    }

    @Override
    public long readLongLE() throws IOException {
      if (pos > endPos - 8) {
        throw new EOFException();
      }
      final long value =
          Long.reverseBytes(
              UNSAFE.getLong(
                  array,
                  (long) pos * Unsafe.ARRAY_BYTE_INDEX_SCALE + Unsafe.ARRAY_BYTE_BASE_OFFSET));
      pos += 8;
      return value;
    }
  }

  private static class LittleEndianNativeOrderByteArrayInput extends ByteArrayInput {

    private LittleEndianNativeOrderByteArrayInput(byte[] array, int offset, int length) {
      super(array, offset, length);
    }

    @Override
    public long readLongLE() throws IOException {
      if (pos > endPos - 8) {
        throw new EOFException();
      }
      final long value =
          UNSAFE.getLong(
              array, (long) pos * Unsafe.ARRAY_BYTE_INDEX_SCALE + Unsafe.ARRAY_BYTE_BASE_OFFSET);
      pos += 8;
      return value;
    }
  }
}
