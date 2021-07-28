/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

public enum BinEncodingMode {
  /**
   * Encodes N bins, each one with its index and its count. Indexes are delta-encoded.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[uvarint64] number of bins N
   *   <li>[varint64] index of first bin
   *   <li>[varfloat64] count of first bin
   *   <li>[varint64] difference between the index of the second bin and the index of the first bin
   *   <li>[varfloat64] count of second bin
   *   <li>...
   *   <li>[varint64] difference between the index of the N-th bin and the index of the (N-1)-th bin
   *   <li>[varfloat64] count of N-th bin
   * </ul>
   */
  INDEX_DELTAS_AND_COUNTS((byte) 1),
  /**
   * Encodes N bins whose counts are each equal to 1. Indexes are delta-encoded.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[uvarint64] number of bins N
   *   <li>[varint64] index of first bin
   *   <li>[varint64] difference between the index of the second bin and the index of the first bin
   *   <li>...
   *   <li>[varint64] difference between the index of the N-th bin and the index of the (N-1)-th bin
   * </ul>
   */
  INDEX_DELTAS((byte) 2),
  /**
   * Encodes N contiguous bins, specifying the count of each one.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[uvarint64] number of bins N
   *   <li>[varint64] index of first bin
   *   <li>[varint64] difference between two successive indexes
   *   <li>[varfloat64] count of first bin
   *   <li>[varfloat64] count of second bin
   *   <li>...
   *   <li>[varfloat64] count of N-th bin
   * </ul>
   */
  CONTIGUOUS_COUNTS((byte) 3);

  private final byte subFlag;

  BinEncodingMode(byte subFlag) {
    this.subFlag = subFlag;
  }

  public final Flag toFlag(Flag.Type storeFlagType) {
    return new Flag(storeFlagType, subFlag);
  }

  public static BinEncodingMode ofFlag(Flag flag) throws InvalidFlagException {
    final int index = (flag.marker() >>> 2) - 1;
    if (index < 0 || index >= values().length) {
      throw new InvalidFlagException("The flag does not encode any valid bin encoding mode.");
    }
    return values()[index];
  }
}
