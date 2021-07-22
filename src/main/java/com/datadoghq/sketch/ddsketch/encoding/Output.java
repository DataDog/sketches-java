/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;

/** A generic interface for writing to a stream or an object. */
public interface Output {

  void writeByte(byte value) throws IOException;

  default void writeLongLE(long value) throws IOException {
    writeByte((byte) value);
    writeByte((byte) (value >> 8));
    writeByte((byte) (value >> 16));
    writeByte((byte) (value >> 24));
    writeByte((byte) (value >> 32));
    writeByte((byte) (value >> 40));
    writeByte((byte) (value >> 48));
    writeByte((byte) (value >> 56));
  }

  default void writeDoubleLE(double value) throws IOException {
    writeLongLE(Double.doubleToRawLongBits(value));
  }
}
