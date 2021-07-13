/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

enum BinsTestCase {
  EMPTY(),
  SINGLE_BIN_COUNT_ONE_0(bin(0, 1)),
  SINGLE_BIN_COUNT_ONE_1(bin(3, 1)),
  SINGLE_BIN_COUNT_ONE_2(bin(Integer.MAX_VALUE >>> 1, 1)),
  SINGLE_BIN_COUNT_ONE_3(bin(-3, 1)),
  SINGLE_BIN_COUNT_ONE_4(bin(Integer.MIN_VALUE >>> 1, 1)),
  SINGLE_BIN_0(bin(0, 0.1)),
  SINGLE_BIN_1(bin(3, 178773.2)),
  SINGLE_BIN_2(bin(Integer.MAX_VALUE >>> 1, 1.2)),
  SINGLE_BIN_3(bin(-3, 3.123)),
  SINGLE_BIN_4(bin(Integer.MIN_VALUE >>> 1, 1e-5)),
  NON_INTEGER_COUNTS_0(
      IntStream.range(0, 10).mapToObj(i -> bin(i, Math.log(i + 1))).toArray(Bin[]::new)),
  NON_INTEGER_COUNTS_1(
      IntStream.range(0, 10).mapToObj(i -> bin(-i, Math.log(i + 1))).toArray(Bin[]::new)),
  INCREASING_LINEARLY(IntStream.range(0, 10000).mapToObj(BinsTestCase::bin).toArray(Bin[]::new)),
  DECREASING_LINEARLY(
      IntStream.range(0, 10000).map(i -> -i).mapToObj(BinsTestCase::bin).toArray(Bin[]::new)),
  INCREASING_EXPONENTIALLY(
      IntStream.range(0, 16)
          .map(i -> (int) Math.pow(2, i))
          .mapToObj(BinsTestCase::bin)
          .toArray(Bin[]::new)),
  DECREASING_EXPONENTIALLY(
      IntStream.range(0, 16)
          .map(i -> -(int) Math.pow(2, i))
          .mapToObj(BinsTestCase::bin)
          .toArray(Bin[]::new)),
  MIN_VALUE(bin(Integer.MIN_VALUE)),
  MAX_VALUE(bin(Integer.MAX_VALUE)),
  LARGE_RANGE_0(bin(0), bin(Integer.MIN_VALUE)),
  LARGE_RANGE_1(bin(0), bin(Integer.MAX_VALUE)),
  LARGE_RANGE_2(bin(Integer.MIN_VALUE), bin(Integer.MAX_VALUE)),
  LARGE_RANGE_3(bin(Integer.MAX_VALUE), bin(Integer.MIN_VALUE));

  private final Bin[] bins;
  private final boolean hasLargeRange;

  BinsTestCase(Bin... bins) {
    this.bins = bins;
    this.hasLargeRange = hasLargeRange(Arrays.asList(bins));
  }

  public List<Bin> getBins() {
    return Arrays.asList(bins);
  }

  public boolean hasLargeRange() {
    return hasLargeRange;
  }

  private static boolean hasLargeRange(Collection<Bin> bins) {
    if (bins.isEmpty()) {
      return false;
    }
    final int minIndex = bins.stream().mapToInt(Bin::getIndex).min().getAsInt();
    final int maxIndex = bins.stream().mapToInt(Bin::getIndex).max().getAsInt();
    return (long) maxIndex - (long) minIndex > Integer.MAX_VALUE >> 1;
  }

  private static Bin bin(int index, double count) {
    return new Bin(index, count);
  }

  private static Bin bin(int index) {
    return bin(index, 1);
  }

  public static Stream<Arguments> argStream() {
    return Arrays.stream(values()).map(Arguments::of);
  }
}
