/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;

public interface Input {

  boolean hasRemaining() throws IOException;

  /**
   * @return the next read byte
   * @throws java.io.EOFException iff a prior call to {@link #hasRemaining()} would have returned true
   */
  byte readByte() throws IOException;

  /**
   * @return the 64-bit integer value built from the next 8 bytes, the least significant byte being first
   * @throws java.io.EOFException iff there are fewer than 8 remaining bytes to read
   */
  default long readLongLE() throws IOException {
    long value = 0;
    value |= Byte.toUnsignedLong(readByte());
    value |= Byte.toUnsignedLong(readByte()) << 8;
    value |= Byte.toUnsignedLong(readByte()) << 16;
    value |= Byte.toUnsignedLong(readByte()) << 24;
    value |= Byte.toUnsignedLong(readByte()) << 32;
    value |= Byte.toUnsignedLong(readByte()) << 40;
    value |= Byte.toUnsignedLong(readByte()) << 48;
    value |= Byte.toUnsignedLong(readByte()) << 56;
    return value;
  }

  default double readDoubleLE() throws IOException {
    return Double.longBitsToDouble(readLongLE());
  }
}
