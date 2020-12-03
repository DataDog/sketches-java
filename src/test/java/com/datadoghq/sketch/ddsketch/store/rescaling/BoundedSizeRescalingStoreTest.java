package com.datadoghq.sketch.ddsketch.store.rescaling;

import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.StoreTest;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BoundedSizeRescalingStoreTest extends StoreTest {

    abstract int maxNumBins();

    @Override
    protected Store newStore() {
        return new BoundedSizeRescalingStore(maxNumBins());
    }

    @Override
    protected Map<Integer, Double> getCounts(Store store, Bin... bins) {

        // FIXME: move elsewhere, avoid casting
        final Store delegate = ((RescalingStore) store).delegate;
        assertTrue(delegate.isEmpty() || delegate.getMaxIndex() - delegate.getMinIndex() <= maxNumBins());

        final int scalingFactor = store.maxShift() + 1;
        return Arrays.stream(bins)
            .map(bin -> new Bin(Math.floorDiv(bin.getIndex(), scalingFactor) * scalingFactor, bin.getCount()))
            .collect(Collectors.groupingBy(
                Bin::getIndex,
                Collectors.summingDouble(Bin::getCount)
            ));
    }

    @Override
    protected void testExtremeValues() {
        // PaginatedStore is not meant to be used with values that are extremely far from one another as it
        // would allocate an excessively large array.
    }

    @Override
    protected void testMergingExtremeValues() {
        // PaginatedStore is not meant to be used with values that are extremely far from one another as it
        // would allocate an excessively large array.
    }

    static class BoundedSizeRescalingStoreTest1 extends BoundedSizeRescalingStoreTest {

        @Override
        int maxNumBins() {
            return 1;
        }
    }

    static class BoundedSizeRescalingStoreTest10 extends BoundedSizeRescalingStoreTest {

        @Override
        int maxNumBins() {
            return 10;
        }
    }

    static class BoundedSizeRescalingStoreTest100 extends BoundedSizeRescalingStoreTest {

        @Override
        int maxNumBins() {
            return 100;
        }
    }
}
