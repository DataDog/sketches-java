/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class is used to perform protobuf serialization compliant with the official schema used to
 * generate protobuf bindings (DDSketch.proto) but does not require the weight of the protobuf-java
 * dependency nor the number of loaded classes required to use protobuf. As such, it can support low
 * overhead use cases such as tracers.
 */
public final class Serializer {

  // any integer type including booleans
  private static final int VARINT = 0;
  // doubles
  private static final int FIXED_64 = 1;
  // strings, binary, arrays (i.e. repeated fields), embedded structs (i.e. messages)
  private static final int LENGTH_DELIMITED = 2;

  private static final int[] VAR_INT_LENGTHS_32 = new int[33];

  static {
    for (int i = 0; i <= 32; ++i) {
      VAR_INT_LENGTHS_32[i] = (31 - i) / 7;
    }
  }

  private final ByteBuffer buffer;

  public Serializer(int size) {
    this.buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
  }

  public ByteBuffer getBuffer() {
    buffer.flip();
    return buffer;
  }

  public void writeHeader(int fieldNumber, int length) {
    writeTag(fieldNumber, LENGTH_DELIMITED);
    writeVarInt(length);
  }

  public void writeCompactArray(int fieldIndex, double[] array, int from, int length) {
    writeTag(fieldIndex, LENGTH_DELIMITED);
    writeVarInt(length * Double.BYTES);
    for (int i = from; i < from + length; ++i) {
      buffer.putDouble(array[i]);
    }
  }

  public void writeDouble(int fieldIndex, double value) {
    if (value != 0D) {
      writeTag(fieldIndex, FIXED_64);
      buffer.putDouble(value);
    }
  }

  public void writeUnsignedInt32(int fieldIndex, int value) {
    if (value != 0) {
      writeTag(fieldIndex, VARINT);
      writeVarInt(value);
    }
  }

  public void writeSignedInt32(int fieldIndex, int value) {
    writeTag(fieldIndex, VARINT);
    writeVarInt(zigZag(value));
  }

  public void writeBin(int fieldPosition, int index, double count) {
    int length = signedIntFieldSize(1, index) + doubleFieldSize(2, count);
    writeHeader(fieldPosition, length);
    writeSignedInt32(1, index);
    writeDouble(2, count);
  }

  void writeTag(int fieldIndex, int wireType) {
    writeVarInt((fieldIndex << 3) | wireType);
  }

  private void writeVarInt(int value) {
    int length = varIntLength(value);
    for (int i = 0; i < length; ++i) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) value);
  }

  // utilities for calculating required buffer sizes

  public static int embeddedSize(int size) {
    return varIntLength(size) + 1 + size;
  }

  public static int fieldSize(int fieldIndex, int value) {
    return value == 0 ? 0 : (tagSize(fieldIndex, VARINT) + varIntLength(value) + 1);
  }

  public static int signedIntFieldSize(int fieldIndex, int value) {
    // zigzag encode
    return tagSize(fieldIndex, VARINT) + varIntLength(zigZag(value)) + 1;
  }

  public static int doubleFieldSize(int fieldIndex, double value) {
    return value == 0D ? 0 : (tagSize(fieldIndex, FIXED_64) + Double.BYTES);
  }

  public static int embeddedFieldSize(int fieldIndex, int size) {
    return tagSize(fieldIndex, LENGTH_DELIMITED) + embeddedSize(size);
  }

  public static int sizeOfCompactDoubleArray(int fieldIndex, int size) {
    return tagSize(fieldIndex, LENGTH_DELIMITED) + embeddedSize(size * Double.BYTES);
  }

  public static int sizeOfBin(int fieldPosition, int index, double count) {
    return embeddedFieldSize(
        fieldPosition, signedIntFieldSize(1, index) + doubleFieldSize(2, count));
  }

  private static int zigZag(int signed) {
    return (signed << 1) ^ (signed >> 31);
  }

  private static int tagSize(int tag, int type) {
    return varIntLength((tag << 3) | type) + 1;
  }

  private static int varIntLength(int value) {
    return VAR_INT_LENGTHS_32[Integer.numberOfLeadingZeros(value)];
  }
}
