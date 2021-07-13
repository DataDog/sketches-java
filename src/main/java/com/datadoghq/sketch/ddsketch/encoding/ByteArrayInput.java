/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

/** An implementation of {@link Input} that is backed by an array. */
public final class ByteArrayInput implements Input {

  private final byte[] array;
  private final int endPos;
  private int pos;

  public ByteArrayInput(byte[] array, int offset, int length) {
    Objects.requireNonNull(array);
    if (offset < 0 || offset + length > array.length) {
      throw new IndexOutOfBoundsException();
    }
    this.array = array;
    this.endPos = offset + length;
    this.pos = offset;
  }

  public ByteArrayInput(byte[] array) {
    this(array, 0, array.length);
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
  public final long readLongLE() throws IOException {
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
}
