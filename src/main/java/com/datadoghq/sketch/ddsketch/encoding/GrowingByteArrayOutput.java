/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.util.Arrays;

/**
 * An implementation of {@link Output} that is backed by an array of bytes, whose capacity is grown
 * as necessary.
 */
public final class GrowingByteArrayOutput implements Output {

  private static final int INITIAL_CAPACITY = 8;

  private byte[] array;
  private int pos = 0; // invariant: pos <= array.length

  private GrowingByteArrayOutput(int initialCapacity) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Capacity cannot be negative");
    }
    this.array = new byte[initialCapacity];
  }

  public static GrowingByteArrayOutput withInitialCapacity(int initialCapacity) {
    return new GrowingByteArrayOutput(initialCapacity);
  }

  public static GrowingByteArrayOutput withDefaultInitialCapacity() {
    return withInitialCapacity(INITIAL_CAPACITY);
  }

  private void grow(int requiredCapacity) {
    final int newCapacity = Math.max(requiredCapacity, array.length << 1);
    final byte[] newArray = new byte[newCapacity];
    System.arraycopy(array, 0, newArray, 0, array.length);
    array = newArray;
  }

  @Override
  public final void writeByte(byte value) {
    if (pos == array.length) {
      grow(pos + 1);
    }
    array[pos++] = value;
  }

  @Override
  public final void writeLongLE(long value) {
    if (pos > array.length - 8) {
      grow(pos + 8);
    }
    array[pos] = (byte) value;
    array[pos + 1] = (byte) (value >> 8);
    array[pos + 2] = (byte) (value >> 16);
    array[pos + 3] = (byte) (value >> 24);
    array[pos + 4] = (byte) (value >> 32);
    array[pos + 5] = (byte) (value >> 40);
    array[pos + 6] = (byte) (value >> 48);
    array[pos + 7] = (byte) (value >> 56);
    pos += 8;
  }

  /** Discard the data that has been written to the backing array but avoid deallocating memory. */
  public final void clear() {
    pos = 0;
  }

  /**
   * @return the array that contains the written data, and possibly some additional trailing bytes;
   *     its {@link #numWrittenBytes()} bytes match what {@link #trimmedCopy()} would return
   */
  public final byte[] backingArray() {
    return array;
  }

  /** @return the number of bytes that have been written to the backing array */
  public final int numWrittenBytes() {
    return pos;
  }

  /**
   * @return an array that is distinct from the backing array that contains the written data (its
   *     size is {@link #numWrittenBytes()})
   */
  public final byte[] trimmedCopy() {
    return Arrays.copyOf(array, pos);
  }
}
