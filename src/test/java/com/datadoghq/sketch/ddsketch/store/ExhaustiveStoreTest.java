package com.datadoghq.sketch.ddsketch.store;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class ExhaustiveStoreTest extends StoreTest {

    @Override
    Map<Integer, Long> getCounts(Collection<Integer> values) {
        return values.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }
}
