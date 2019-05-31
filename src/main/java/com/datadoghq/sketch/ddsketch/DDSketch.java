package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.CollapsingMaxDenseStore;
import com.datadoghq.sketch.ddsketch.store.CollapsingMinDenseStore;
import com.datadoghq.sketch.ddsketch.store.SparseStore;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class DDSketch {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexableValue;
    private final Store store;
    private long zeroCount;

    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
        this(indexMapping, storeSupplier, 0);
    }

    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier, double minIndexedValue) {
        this.indexMapping = indexMapping;
        this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
        this.maxIndexableValue = indexMapping.maxIndexableValue();
        this.store = storeSupplier.get();
        this.zeroCount = 0L;
    }

    private DDSketch(DDSketch sketch) {
        this.indexMapping = sketch.indexMapping;
        this.minIndexedValue = sketch.minIndexedValue;
        this.maxIndexableValue = sketch.maxIndexableValue;
        this.store = sketch.store.copy();
        this.zeroCount = sketch.zeroCount;
    }

    public IndexMapping getIndexMapping() {
        return indexMapping;
    }

    public double getMin() {
        return indexMapping.value(store.getMinIndex());
    }

    public double getMax() {
        return indexMapping.value(store.getMaxIndex());
    }

    public long getTotalCount() {
        return store.getTotalCount() + zeroCount;
    }

    public void add(double value) {

        checkValueTrackable(value);

        if (value < minIndexedValue) {
            zeroCount++;
        } else {
            store.add(indexMapping.index(value));
        }
    }

    public void add(double value, long count) {

        checkValueTrackable(value);

        if (count < 0) {
            throw new IllegalArgumentException("negative input count");
        }

        if (value < minIndexedValue) {
            zeroCount += count;
        } else {
            store.add(indexMapping.index(value), count);
        }
    }

    private void checkValueTrackable(double value) {

        if (value < 0 || value > maxIndexableValue) {
            throw new IllegalArgumentException("input value outside trackable range");
        }
    }

    public void mergeWith(DDSketch other) {

        if (!indexMapping.equals(other.indexMapping)) {
            throw new IllegalArgumentException("unmergeable sketches");
        }

        store.mergeWith(other.store);
        zeroCount += other.zeroCount;
    }

    public double getValueAtQuantile(double quantile) {
        return getValueAtQuantile(quantile, getTotalCount());
    }

    public double[] getValuesAtQuantiles(double[] quantiles) {
        final long totalCount = getTotalCount();
        return Arrays.stream(quantiles)
                .map(quantile -> getValueAtQuantile(quantile, totalCount))
                .toArray();
    }

    double getValueAtQuantile(double quantile, long totalCount) {

        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("invalid quantile");
        }

        if (totalCount == 0L) {
            throw new NoSuchElementException();
        }

        final long rank = (long) (quantile * (totalCount - 1));
        if (rank < zeroCount) {
            return 0;
        }

        Bin bin;
        if (quantile <= 0.5) {

            final Iterator<Bin> binIterator = store.getAscendingBinIterator();
            long n = zeroCount;
            do {
                bin = binIterator.next();
                n += bin.getCount();
            } while (n <= rank && binIterator.hasNext());

        } else {

            final Iterator<Bin> binIterator = store.getDescendingBinIterator();
            long n = totalCount;
            do {
                bin = binIterator.next();
                n -= bin.getCount();
            } while (n > rank && binIterator.hasNext());
        }

        return indexMapping.value(bin.getIndex());
    }

    public DDSketch copy() {
        return new DDSketch(this);
    }

    // Preset sketches

    // For fast insertion

    public static DDSketch fast(double relativeAccuracy) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    public static DDSketch fastCollapsingMin(double relativeAccuracy, int maxNumBuckets) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingMinDenseStore(maxNumBuckets)
        );
    }

    public static DDSketch fastCollapsingMax(double relativeAccuracy, int maxNumBuckets) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingMaxDenseStore(maxNumBuckets)
        );
    }

    // For relatively fast insertion but smaller memory footprint

    public static DDSketch memoryOptimal(double relativeAccuracy) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    public static DDSketch memoryOptimalCollapsingMin(double relativeAccuracy, int maxNumBuckets) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                () -> new CollapsingMinDenseStore(maxNumBuckets)
        );
    }

    public static DDSketch memoryOptimalCollapsingMax(double relativeAccuracy, int maxNumBuckets) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                () -> new CollapsingMaxDenseStore(maxNumBuckets)
        );
    }

    // For light memory footprint in the case of sparse data sets

    public static DDSketch sparse(double relativeAccuracy) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                SparseStore::new
        );
    }
}
