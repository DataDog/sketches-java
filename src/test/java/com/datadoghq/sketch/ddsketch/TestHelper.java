/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.assertj.core.util.DoubleComparator;
import org.junit.jupiter.params.provider.Arguments;

public final class TestHelper {

  public static final RecursiveComparisonConfiguration BIN_COMPARISON_CONFIG =
      RecursiveComparisonConfiguration.builder()
          .withIgnoredOverriddenEqualsForTypes(Bin.class)
          .withComparatorForType(
              new DoubleComparator(AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR), Double.class)
          .build();

  public static Stream<Arguments> product(List<Stream<Arguments>> streams) {
    if (streams.isEmpty()) {
      return Stream.of(Arguments.of());
    }
    final List<Arguments> firstArguments = streams.get(0).collect(Collectors.toList());
    return product(streams.subList(1, streams.size()))
        .flatMap(
            other ->
                firstArguments.stream()
                    .map(
                        first ->
                            Arguments.of(
                                Stream.concat(
                                        Arrays.stream(first.get()), Arrays.stream(other.get()))
                                    .toArray())));
  }

  @SafeVarargs
  public static Stream<Arguments> product(Stream<Arguments>... streams) {
    return product(Arrays.asList(streams));
  }

  private TestHelper() {}
}
