/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;

abstract class CollapsingLowestDenseStoreTest extends StoreTest {

  abstract int maxNumBins();

  @Override
  Store newStore() {
    return new CollapsingLowestDenseStore(maxNumBins());
  }

  @Override
  Map<Integer, Double> getCounts(Bin... bins) {
    final OptionalInt maxIndex =
        Arrays.stream(bins).filter(bin -> bin.getCount() > 0).mapToInt(Bin::getIndex).max();
    if (!maxIndex.isPresent()) {
      return Collections.emptyMap();
    }
    final int minStorableIndex =
        (int) Math.max(Integer.MIN_VALUE, (long) maxIndex.getAsInt() - maxNumBins() + 1);
    return Arrays.stream(bins)
        .collect(
            Collectors.groupingBy(
                bin -> Math.max(bin.getIndex(), minStorableIndex),
                Collectors.summingDouble(Bin::getCount)));
  }

  static class CollapsingLowestDenseStoreTest1 extends CollapsingLowestDenseStoreTest {

    @Override
    int maxNumBins() {
      return 1;
    }
  }

  static class CollapsingLowestDenseStoreTest20 extends CollapsingLowestDenseStoreTest {

    @Override
    int maxNumBins() {
      return 20;
    }
  }

  static class CollapsingLowestDenseStoreTest1000 extends CollapsingLowestDenseStoreTest {

    @Override
    int maxNumBins() {
      return 1000;
    }
  }
}
