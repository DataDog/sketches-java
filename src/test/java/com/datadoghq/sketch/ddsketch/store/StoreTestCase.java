/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public enum StoreTestCase {
  PAGINATED(PaginatedStore::new, UnaryOperator.identity(), false),
  SPARSE(SparseStore::new, UnaryOperator.identity(), true),
  DENSE_UNBOUNDED(UnboundedSizeDenseStore::new, UnaryOperator.identity(), false),
  DENSE_COLLAPSING_LOWEST_100(() -> new CollapsingLowestDenseStore(100), collapseLowest(100), true),
  DENSE_COLLAPSING_HIGHEST_100(
      () -> new CollapsingHighestDenseStore(100), collapseHighest(100), true);

  private final Supplier<Store> storeSupplier;
  private final UnaryOperator<Collection<Bin>> binTransformer; // does not necessarily return a copy
  private final boolean acceptsLargeRange;

  StoreTestCase(
      Supplier<Store> storeSupplier,
      UnaryOperator<Collection<Bin>> binTransformer,
      boolean acceptsLargeRange) {
    this.storeSupplier = storeSupplier;
    this.binTransformer = binTransformer;
    this.acceptsLargeRange = acceptsLargeRange;
  }

  public Supplier<Store> storeSupplier() {
    return storeSupplier;
  }

  public UnaryOperator<Collection<Bin>> binTransformer() {
    return binTransformer;
  }

  public boolean isLossless() {
    // Lossless cases always use UnaryOperator.identity()
    return UnaryOperator.identity().equals(binTransformer);
  }

  public boolean acceptsLargeRange() {
    return acceptsLargeRange;
  }

  private static UnaryOperator<Collection<Bin>> collapseLowest(final int maxNumBins) {
    if (maxNumBins <= 0) {
      throw new IllegalArgumentException();
    }
    return bins -> {
      final OptionalInt maxIndex = bins.stream().mapToInt(Bin::getIndex).max();
      if (!maxIndex.isPresent()) {
        return bins;
      }
      final int lowerBound =
          Integer.MIN_VALUE + maxNumBins > maxIndex.getAsInt()
              ? Integer.MIN_VALUE
              : maxIndex.getAsInt() - maxNumBins + 1;
      return bins.stream()
          .map(bin -> new Bin(Math.max(bin.getIndex(), lowerBound), bin.getCount()))
          .collect(Collectors.toList());
    };
  }

  private static UnaryOperator<Collection<Bin>> collapseHighest(final int maxNumBins) {
    if (maxNumBins <= 0) {
      throw new IllegalArgumentException();
    }
    return bins -> {
      final OptionalInt minIndex = bins.stream().mapToInt(Bin::getIndex).min();
      if (!minIndex.isPresent()) {
        return bins;
      }
      final int upperBound =
          Integer.MAX_VALUE - maxNumBins < minIndex.getAsInt()
              ? Integer.MAX_VALUE
              : minIndex.getAsInt() + maxNumBins - 1;
      return bins.stream()
          .map(bin -> new Bin(Math.min(bin.getIndex(), upperBound), bin.getCount()))
          .collect(Collectors.toList());
    };
  }

  public static Stream<Arguments> argStream() {
    return Arrays.stream(values()).map(Arguments::of);
  }
}
