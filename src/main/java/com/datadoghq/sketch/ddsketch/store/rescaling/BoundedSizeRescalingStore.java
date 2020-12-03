package com.datadoghq.sketch.ddsketch.store.rescaling;

import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.util.function.Supplier;

/**
 * A store with bounded memory size that merges contiguous buckets if necessary, hence uniformly degrading the accuracy
 * of the sketch that uses it.
 * <p>
 * Bucket merging happens when rescaling the store. The rescaling factor is always a power of 2, so that the size of the
 * merged buckets (that is to say the number of contiguous indices that are mapped to the same counter) can be expressed
 * as \(2^n\). The relative accuracy of a sketch that uses such a store is \(\alpha^{2^n}\), where \(\alpha\) is the
 * relative accuracy of the index mapping that the store uses.
 */
public class BoundedSizeRescalingStore extends RescalingStore {

    private final int maxNumBins;

    public BoundedSizeRescalingStore(int maxNumBins) {
        super(UnboundedSizeDenseStore::new);
        this.maxNumBins = maxNumBins;
    }

    private BoundedSizeRescalingStore(Supplier<? extends Store> storeSupplier, Store delegate, int scalingFactor, int maxNumBins) {
        super(storeSupplier, delegate, scalingFactor);
        this.maxNumBins = maxNumBins;
    }

    @Override
    protected int shouldRescale() {
        if (delegate.isEmpty()) {
            return 1;
        }
        // FIXME: this is not the actual number of allocated bins
        final int span = delegate.getMaxIndex() - delegate.getMinIndex();
        if (span > maxNumBins) {
            // Rescale by the smallest possible power of 2
            int rescalingFactor = 2;
            while (span > maxNumBins * rescalingFactor) {
                rescalingFactor = Math.multiplyExact(rescalingFactor, 2);
            }
            return rescalingFactor;
        } else {
            return 1;
        }
    }

    @Override
    public Store copy() {
        return new BoundedSizeRescalingStore(storeSupplier, delegate.copy(), scalingFactor, maxNumBins);
    }
}
