/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static com.datadoghq.sketch.ddsketch.TestHelper.BIN_COMPARISON_CONFIG;
import static com.datadoghq.sketch.ddsketch.TestHelper.product;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.BinAcceptor;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IndexMappingConverterTest {

  private static final Offset<Double> DOUBLE_OFFSET =
      offset(AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);

  private static BinAcceptor listAdder(List<Bin> binList) {
    return (index, value) -> binList.add(new Bin(index, value));
  }

  @ParameterizedTest
  @MethodSource("mappingAndBins")
  void testSameMapping(IndexMapping mapping, List<Bin> bins) {
    final List<Bin> outBins = new ArrayList<>();
    IndexMappingConverter.distributingUniformly(mapping, mapping)
        .convertAscendingIterator(bins.iterator(), listAdder(outBins));

    assertThat(outBins).usingRecursiveComparison(BIN_COMPARISON_CONFIG).isEqualTo(bins);
  }

  @ParameterizedTest
  @MethodSource("twoMappingsAndBins")
  void testDistinctMappings(IndexMapping inMapping, IndexMapping outMapping, List<Bin> bins) {
    final List<Bin> outBins = new ArrayList<>();
    IndexMappingConverter.distributingUniformly(inMapping, outMapping)
        .convertAscendingIterator(bins.iterator(), listAdder(outBins));

    final List<Integer> indexes = outBins.stream().map(Bin::getIndex).collect(Collectors.toList());

    assertThat(indexes).isSorted();
    assertThat(indexes).doesNotHaveDuplicates();

    assertThat(outBins.stream().mapToDouble(Bin::getCount).sum())
        .isCloseTo(bins.stream().mapToDouble(Bin::getCount).sum(), DOUBLE_OFFSET);
  }

  @ParameterizedTest
  @MethodSource("bins")
  void testMergingContiguousBins(List<Bin> bins) {
    // One bucket of outMapping covers exactly two contiguous buckets of inMapping.
    final BitwiseLinearlyInterpolatedMapping inMapping = new BitwiseLinearlyInterpolatedMapping(4);
    final BitwiseLinearlyInterpolatedMapping outMapping = new BitwiseLinearlyInterpolatedMapping(3);

    final List<Bin> outBins = new ArrayList<>();
    IndexMappingConverter.distributingUniformly(inMapping, outMapping)
        .convertAscendingIterator(bins.iterator(), listAdder(outBins));

    final List<Bin> mergedBins =
        bins.stream()
            .map(bin -> new Bin(Math.floorDiv(bin.getIndex(), 2), bin.getCount()))
            .collect(
                Collectors.groupingBy(
                    Bin::getIndex,
                    Collectors.mapping(Bin::getCount, Collectors.reducing(0D, Double::sum))))
            .entrySet()
            .stream()
            .map(entry -> new Bin(entry.getKey(), entry.getValue()))
            .sorted(Comparator.comparing(Bin::getIndex))
            .collect(Collectors.toList());

    assertThat(outBins).usingRecursiveComparison(BIN_COMPARISON_CONFIG).isEqualTo(mergedBins);
  }

  @ParameterizedTest
  @MethodSource("bins")
  void testIndexShifting(List<Bin> bins) {
    final double gamma = 1.05;
    final int indexShift = 4;
    final LogarithmicMapping inMapping = new LogarithmicMapping(gamma, 0);
    final LogarithmicMapping outMapping = new LogarithmicMapping(gamma, indexShift);

    final List<Bin> outBins = new ArrayList<>();
    IndexMappingConverter.distributingUniformly(inMapping, outMapping)
        .convertAscendingIterator(bins.iterator(), listAdder(outBins));

    final List<Bin> shiftedBins =
        bins.stream()
            .map(bin -> new Bin(bin.getIndex() + indexShift, bin.getCount()))
            .collect(Collectors.toList());

    assertThat(outBins).usingRecursiveComparison(BIN_COMPARISON_CONFIG).isEqualTo(shiftedBins);
  }

  static Stream<Arguments> mappingAndBins() {
    return product(mappings(), bins());
  }

  static Stream<Arguments> twoMappingsAndBins() {
    return product(mappings(), mappings(), bins());
  }

  static Stream<Arguments> mappings() {
    return Stream.of(
            new LogarithmicMapping(1e-2),
            new LogarithmicMapping(1.03, 4),
            new LogarithmicMapping(5e-4),
            new CubicallyInterpolatedMapping(1.5e-2),
            new BitwiseLinearlyInterpolatedMapping(1e-3))
        .map(Arguments::arguments);
  }

  static Stream<Arguments> bins() {
    return Stream.of(
            Collections.singletonList(new Bin(1, 1)),
            Arrays.asList(new Bin(1, 1), new Bin(2, 1), new Bin(3, 1)),
            Arrays.asList(new Bin(1, 0.3), new Bin(2, 1.6)),
            Arrays.asList(new Bin(-1, 0.3), new Bin(3, 1.6)),
            Arrays.asList(
                new Bin(-2, 0.3),
                new Bin(-1, 1.6),
                new Bin(0, 0.1),
                new Bin(2, 65),
                new Bin(3, 5.43)))
        .map(Arguments::arguments);
  }
}
