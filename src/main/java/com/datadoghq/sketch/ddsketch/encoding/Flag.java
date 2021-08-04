/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

import java.io.IOException;
import java.util.Objects;

/**
 * An encoded DDSketch comprises multiple contiguous blocks (sequences of bytes). Each block is
 * prefixed with a flag that indicates what the block contains and how the data is encoded in the
 * block.
 *
 * <p>A flag is a encoded with a single byte, which itself contains two parts:
 *
 * <ul>
 *   <li>the flag type (the 2 least significant bits),
 *   <li>the subflag (the 6 most significant bits).
 * </ul>
 *
 * <p>There are four flag types, for:
 *
 * <ul>
 *   <li>the sketch features,
 *   <li>the index mapping,
 *   <li>the positive value store,
 *   <li>the negative value store.
 * </ul>
 *
 * <p>The meaning of the subflag depends on the flag type:
 *
 * <ul>
 *   <li>for the sketch feature flag type, it indicates what feature is encoded,
 *   <li>for the index mapping flag type, it indicates what mapping is encoded and how,
 *   <li>for the store flag types, it indicates how bins are encoded.
 * </ul>
 */
public final class Flag {

  /**
   * Encodes the count of the zero bin.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[varfloat64] count of the zero bin
   * </ul>
   */
  public static final Flag ZERO_COUNT = new Flag(Type.SKETCH_FEATURES, (byte) 1);

  /**
   * Encodes the total count.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[varfloat64] total count
   * </ul>
   */
  public static final Flag COUNT = new Flag(Type.SKETCH_FEATURES, (byte) 0x28);

  /**
   * Encodes the total sum.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] total sum
   * </ul>
   */
  public static final Flag SUM = new Flag(Type.SKETCH_FEATURES, (byte) 0x21);

  /**
   * Encodes the global minimum value.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] global minimum value
   * </ul>
   */
  public static final Flag MIN = new Flag(Type.SKETCH_FEATURES, (byte) 0x22);

  /**
   * Encodes the global maximum value.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] global maximum value
   * </ul>
   */
  public static final Flag MAX = new Flag(Type.SKETCH_FEATURES, (byte) 0x23);

  private final byte marker;

  private Flag(byte marker) {
    this.marker = marker;
  }

  Flag(Type type, byte subFlag) {
    this((byte) (type.ordinal() | (subFlag << 2)));
  }

  final byte marker() {
    return marker;
  }

  public Type type() {
    return Type.values()[marker & 3];
  }

  public void encode(Output output) throws IOException {
    output.writeByte(marker);
  }

  /**
   * Return a flag built from the data read from the provided {@code Input}.
   *
   * <p>Note that for performance reasons, there is no validation happening in this method while
   * decoding the flag. As a consequence, the returned flag may not actually be a valid flag.
   *
   * @param input where to read from
   * @return the flag built from the data immediately read from the {@code input}
   * @throws IOException if an IO exception is thrown while reading from the {@code input}
   */
  public static Flag decode(Input input) throws IOException {
    return new Flag(input.readByte());
  }

  @Override
  public boolean equals(Object o) {
    return o != null && getClass() == o.getClass() && marker == ((Flag) o).marker;
  }

  @Override
  public int hashCode() {
    return Objects.hash(marker);
  }

  public enum Type {
    SKETCH_FEATURES, // 0b00
    POSITIVE_STORE, // 0b01
    INDEX_MAPPING, // 0b10
    NEGATIVE_STORE // 0b11
  }
}
