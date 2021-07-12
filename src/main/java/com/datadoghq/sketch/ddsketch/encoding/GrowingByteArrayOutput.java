/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

public final class GrowingByteArrayOutput implements Output {

  private static final int INITIAL_CAPACITY = 8;

  private byte[] array;
  private int pos = 0; // invariant: pos <= array.length

  public GrowingByteArrayOutput(int initialCapacity) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Capacity cannot be negative");
    }
    this.array = new byte[initialCapacity];
  }

  public GrowingByteArrayOutput() {
    this.array = new byte[INITIAL_CAPACITY];
  }

  private void grow(int requiredAdditionalCapacity) {
    final int newCapacity = Math.max(array.length + requiredAdditionalCapacity, array.length << 1);
    final byte[] newArray = new byte[newCapacity];
    System.arraycopy(array, 0, newArray, 0, array.length);
    array = newArray;
  }

  @Override
  public final void writeByte(byte value) {
    if (pos == array.length) {
      grow(1);
    }
    array[pos++] = value;
  }

  @Override
  public final void writeLongLE(long value) {
    if (pos >= array.length - 8) {
      grow(8);
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

  public final void clear() {
    pos = 0;
  }

  public final byte[] backingArray() {
    return array;
  }

  public final int numWrittenBytes() {
    return pos;
  }

  public final byte[] trimmedCopy() {
    final byte[] copy = new byte[pos];
    System.arraycopy(array, 0, copy, 0, copy.length);
    return copy;
  }
}
