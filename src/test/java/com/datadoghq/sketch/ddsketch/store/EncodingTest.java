/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import static com.datadoghq.sketch.ddsketch.TestHelper.BIN_COMPARISON_CONFIG;
import static com.datadoghq.sketch.ddsketch.TestHelper.product;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.ddsketch.encoding.BinEncodingMode;
import com.datadoghq.sketch.ddsketch.encoding.ByteArrayInput;
import com.datadoghq.sketch.ddsketch.encoding.Flag;
import com.datadoghq.sketch.ddsketch.encoding.GrowingByteArrayOutput;
import com.datadoghq.sketch.ddsketch.encoding.Input;
import com.datadoghq.sketch.ddsketch.encoding.Output;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncodingTest {

  private static final Flag.Type STORE_FLAG_TYPE = Flag.Type.POSITIVE_STORE;

  @ParameterizedTest
  @MethodSource("binsAndStoreTestCases")
  void testEncodeDecode(
      BinsTestCase binsTestCase,
      StoreTestCase initialStoreTestCase,
      StoreTestCase finalStoreTestCase) {
    final Store initialStore = initialStoreTestCase.storeSupplier().get();
    binsTestCase.getBins().forEach(initialStore::add);
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    encode(output, initialStore);
    final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
    final Store finalStore = finalStoreTestCase.storeSupplier().get();
    decode(input, finalStore);
    final Collection<Bin> transformedBins =
        (initialStoreTestCase
            .binTransformer()
            .andThen(finalStoreTestCase.binTransformer())
            .apply(binsTestCase.getBins()));
    assertThat(finalStore.getStream())
        .usingRecursiveComparison(BIN_COMPARISON_CONFIG)
        .isEqualTo(normalize(transformedBins));
  }

  static Stream<Arguments> binsAndStoreTestCases() {
    return product(BinsTestCase.argStream(), StoreTestCase.argStream(), StoreTestCase.argStream())
        .filter(
            arguments -> {
              final BinsTestCase binsTestCase = (BinsTestCase) arguments.get()[0];
              final StoreTestCase initialStoreTestCase = (StoreTestCase) arguments.get()[1];
              final StoreTestCase finalStoreTestCase = (StoreTestCase) arguments.get()[2];
              return !binsTestCase.hasLargeRange()
                  || (initialStoreTestCase.acceptsLargeRange()
                      && finalStoreTestCase.acceptsLargeRange());
            });
  }

  private static List<Bin> normalize(Collection<Bin> bins) {
    final Map<Integer, Double> groupedByIndex =
        bins.stream()
            .collect(
                Collectors.groupingBy(
                    Bin::getIndex,
                    Collectors.mapping(Bin::getCount, Collectors.reducing(0.0, Double::sum))));
    return groupedByIndex.entrySet().stream()
        .filter(entry -> entry.getValue() != 0)
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> new Bin(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private static void encode(Output output, Store store) {
    try {
      store.encode(output, STORE_FLAG_TYPE);
    } catch (IOException e) {
      fail(e);
    }
  }

  private static void decode(Input input, Store store) {
    try {
      while (input.hasRemaining()) {
        final Flag flag = Flag.decode(input);
        if (!STORE_FLAG_TYPE.equals(flag.type())) {
          fail("Invalid flag type");
        }
        store.decodeAndMergeWith(input, BinEncodingMode.ofFlag(flag));
      }
    } catch (IOException e) {
      fail(e);
    }
  }
}
