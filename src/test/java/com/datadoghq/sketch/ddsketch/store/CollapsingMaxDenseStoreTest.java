package com.datadoghq.sketch.ddsketch.store;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

abstract class CollapsingMaxDenseStoreTest extends StoreTest {

    abstract int maxNumBuckets();

    @Override
    Store newStore() {
        return new CollapsingMaxDenseStore(maxNumBuckets());
    }

    @Override
    Map<Integer, Long> getCounts(Collection<Integer> values) {
        if (values.isEmpty()) {
            return Map.of();
        }
        final int minValue = values.stream().mapToInt(i -> i).min().getAsInt();
        final int maxStorableValue = minValue + maxNumBuckets() - 1;
        return values.stream().collect(Collectors.groupingBy(
                i -> Math.min(i, maxStorableValue),
                Collectors.counting())
        );
    }

    static class CollapsingMaxDenseStoreTest1 extends CollapsingMaxDenseStoreTest {

        @Override
        int maxNumBuckets() {
            return 1;
        }
    }

    static class CollapsingMaxDenseStoreTest20 extends CollapsingMaxDenseStoreTest {

        @Override
        int maxNumBuckets() {
            return 20;
        }
    }

    static class CollapsingMaxDenseStoreTest1000 extends CollapsingMaxDenseStoreTest {

        @Override
        int maxNumBuckets() {
            return 1000;
        }
    }
}
