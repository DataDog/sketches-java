/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.encoding;

public enum IndexMappingLayout {
  /**
   * Encodes the logarithmic index mapping, specifying the base \(\gamma\) and the index offset.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] gamma
   *   <li>[float64LE] index offset
   * </ul>
   */
  LOG,
  /**
   * Encodes the logarithmic index mapping with linear interpolation between powers of 2, specifying
   * the base \(\gamma\) and the index offset.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] gamma
   *   <li>[float64LE] index offset
   * </ul>
   */
  LOG_LINEAR,
  /**
   * Encodes the logarithmic index mapping with quadratic interpolation between powers of 2,
   * specifying the base \(\gamma\) and the index offset.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] gamma
   *   <li>[float64LE] index offset
   * </ul>
   */
  LOG_QUADRATIC,
  /**
   * Encodes the logarithmic index mapping with cubic interpolation between powers of 2, specifying
   * the base \(\gamma\) and the index offset.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] gamma
   *   <li>[float64LE] index offset
   * </ul>
   */
  LOG_CUBIC,
  /**
   * Encodes the logarithmic index mapping with wuartic interpolation between powers of 2,
   * specifying the base \(\gamma\) and the index offset.
   *
   * <p>Encoding format:
   *
   * <ul>
   *   <li>[byte] flag
   *   <li>[float64LE] gamma
   *   <li>[float64LE] index offset
   * </ul>
   */
  LOG_QUARTIC;

  private final Flag flag;

  IndexMappingLayout() {
    this.flag = new Flag(Flag.Type.INDEX_MAPPING, (byte) ordinal());
  }

  public final Flag toFlag() {
    return flag;
  }

  public static IndexMappingLayout ofFlag(Flag flag) throws InvalidFlagException {
    final int index = flag.marker() >>> 2;
    if (index >= values().length) {
      throw new InvalidFlagException("The flag does not encode any valid index mapping layout.");
    }
    return values()[index];
  }
}
