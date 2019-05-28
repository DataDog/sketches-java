package com.datadoghq.sketch.ddsketch.store;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

abstract class CollapsingMinDenseStoreTest extends StoreTest {

    abstract int maxNumBuckets();

    @Override
    Store newStore() {
        return new CollapsingMinDenseStore(maxNumBuckets());
    }

    @Override
    Map<Integer, Long> getCounts(Collection<Integer> values) {
        if (values.isEmpty()) {
            return Map.of();
        }
        final int maxValue = values.stream().mapToInt(i -> i).max().getAsInt();
        final int minStorableValue = maxValue - maxNumBuckets() + 1;
        return values.stream().collect(Collectors.groupingBy(
                i -> Math.max(i, minStorableValue),
                Collectors.counting())
        );
    }

    static class CollapsingMinDenseStoreTest1 extends CollapsingMinDenseStoreTest {

        @Override
        int maxNumBuckets() {
            return 1;
        }
    }

    static class CollapsingMinDenseStoreTest20 extends CollapsingMinDenseStoreTest {

        @Override
        int maxNumBuckets() {
            return 20;
        }
    }

    static class CollapsingMinDenseStoreTest1000 extends CollapsingMinDenseStoreTest {

        @Override
        int maxNumBuckets() {
            return 1000;
        }
    }
}
