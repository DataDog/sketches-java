/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.mapping.QuadraticallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.CollapsingHighestDenseStore;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A {@link QuantileSketch} with relative-error guarantees. This sketch computes quantile values with an
 * approximation error that is relative to the actual quantile value. It works on non-negative input values.
 * <p>
 * For instance, using a {@code DDSketch} with a relative accuracy guarantee set to 1%, if the expected quantile
 * value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value
 * is 1000, the computed quantile value is guaranteed to be between 990 and 1010.
 * <p>
 * A {@code DDSketch} works by mapping floating-point input values to bins and counting the number of values for each
 * bin. The mapping to bins is handled by {@link IndexMapping}, while the underlying structure that keeps track of
 * bin counts is {@link Store}. The standard parameters of the sketch, provided by {@link #standard}, should work in
 * most cases. For using a specific {@link IndexMapping} or a specific implementation of {@link Store}, the
 * constructor can be used ({@link #DDSketch(IndexMapping, Supplier)}).
 * <p>
 * The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the
 * more bins are needed to keep track of the input values. As a rough estimate, if working on durations using
 * {@link #standard} with a relative accuracy of 2%, about 2.2kB (297 bins) are needed to cover values between 1
 * millisecond and 1 minute, and about 6.8kB (867 bins) to cover values between 1 nanosecond and 1 day. The number of
 * bins that are maintained can be upper-bounded using collapsing stores (see for example
 * {@link #standardCollapsingLowest} and {@link #standardCollapsingHighest}).
 * <p>
 * Note that this implementation is not thread-safe.
 */
public class DDSketch implements QuantileSketch<DDSketch> {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexedValue;

    private final Store store;
    private long zeroCount;

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     * @see #standard
     * @see #standardCollapsingLowest
     * @see #standardCollapsingHighest
     * @see #fast
     * @see #fastCollapsingLowest
     * @see #fastCollapsingHighest
     * @see #memoryOptimal
     * @see #memoryOptimalCollapsingLowest
     * @see #memoryOptimalCollapsingHighest
     */
    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
        this(indexMapping, storeSupplier, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     * @param minIndexedValue the least value that should be distinguished from zero
     * @see #standard
     * @see #standardCollapsingLowest
     * @see #standardCollapsingHighest
     * @see #fast
     * @see #fastCollapsingLowest
     * @see #fastCollapsingHighest
     * @see #memoryOptimal
     * @see #memoryOptimalCollapsingLowest
     * @see #memoryOptimalCollapsingHighest
     */
    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier, double minIndexedValue) {
        this.indexMapping = indexMapping;
        this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
        this.maxIndexedValue = indexMapping.maxIndexableValue();
        this.store = storeSupplier.get();
        this.zeroCount = 0;
    }

    private DDSketch(DDSketch sketch) {
        this.indexMapping = sketch.indexMapping;
        this.minIndexedValue = sketch.minIndexedValue;
        this.maxIndexedValue = sketch.maxIndexedValue;
        this.store = sketch.store.copy();
        this.zeroCount = sketch.zeroCount;
    }

    public IndexMapping getIndexMapping() {
        return indexMapping;
    }

    public Store getStore() {
        return store;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the value is outside the range that is tracked by the sketch
     */
    @Override
    public void accept(double value) {

        checkValueTrackable(value);

        if (value < minIndexedValue) {
            zeroCount++;
        } else {
            store.add(indexMapping.index(value));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the value is outside the range that is tracked by the sketch
     */
    @Override
    public void accept(double value, long count) {

        checkValueTrackable(value);

        if (value < minIndexedValue) {
            if (count < 0) {
                throw new IllegalArgumentException("The count cannot be negative.");
            }
            zeroCount += count;
        } else {
            store.add(indexMapping.index(value), count);
        }
    }

    private void checkValueTrackable(double value) {

        if (value < 0 || value > maxIndexedValue) {
            throw new IllegalArgumentException("The input value is outside the range that is tracked by the sketch.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the other sketch does not use the same index mapping
     */
    @Override
    public void mergeWith(DDSketch other) {

        if (!indexMapping.equals(other.indexMapping)) {
            throw new IllegalArgumentException(
                "The sketches are not mergeable because they do not use the same index mappings."
            );
        }

        store.mergeWith(other.store);
        zeroCount += other.zeroCount;
    }

    @Override
    public DDSketch copy() {
        return new DDSketch(this);
    }

    @Override
    public boolean isEmpty() {
        return zeroCount == 0 && store.isEmpty();
    }

    @Override
    public long getCount() {
        return zeroCount + store.getTotalCount();
    }

    @Override
    public double getMinValue() {
        if (zeroCount > 0) {
            return 0;
        } else {
            return indexMapping.value(store.getMinIndex());
        }
    }

    @Override
    public double getMaxValue() {
        if (zeroCount > 0 && store.isEmpty()) {
            return 0;
        } else {
            return indexMapping.value(store.getMaxIndex());
        }
    }

    @Override
    public double getValueAtQuantile(double quantile) {
        return getValueAtQuantile(quantile, getCount());
    }

    @Override
    public double[] getValuesAtQuantiles(double[] quantiles) {
        final long count = getCount();
        return Arrays.stream(quantiles)
            .map(quantile -> getValueAtQuantile(quantile, count))
            .toArray();
    }

    private double getValueAtQuantile(double quantile, long count) {

        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("The quantile must be between 0 and 1.");
        }

        if (count == 0) {
            throw new NoSuchElementException();
        }

        final long rank = (long) (quantile * (count - 1));
        if (rank < zeroCount) {
            return 0;
        }

        Bin bin;
        if (quantile <= 0.5) {

            final Iterator<Bin> binIterator = store.getAscendingIterator();
            long n = zeroCount;
            do {
                bin = binIterator.next();
                n += bin.getCount();
            } while (n <= rank && binIterator.hasNext());

        } else {

            final Iterator<Bin> binIterator = store.getDescendingIterator();
            long n = count;
            do {
                bin = binIterator.next();
                n -= bin.getCount();
            } while (n > rank && binIterator.hasNext());
        }

        return indexMapping.value(bin.getIndex());
    }

    // Preset sketches

    /**
     * Constructs a standard {@code DDSketch} (with high ingestion speed and low memory footprint).
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     */
    public static DDSketch standard(double relativeAccuracy) {
        return new DDSketch(
            new QuadraticallyInterpolatedMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a standard {@code DDSketch} (with high ingestion speed and low memory footprint), using a limited
     * number of bins. When the maximum number of bins is reached, bins with lowest indices are collapsed, which
     * causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     */
    public static DDSketch standardCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new QuadraticallyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a standard {@code DDSketch} (with high ingestion speed and low memory footprint), using a limited
     * number of bins. When the maximum number of bins is reached, bins with highest indices are collapsed, which
     * causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     */
    public static DDSketch standardCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new QuadraticallyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast {@code DDSketch} (with optimized ingestion speed, at the cost of higher memory usage).
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     */
    public static DDSketch fast(double relativeAccuracy) {
        return new DDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a fast {@code DDSketch} (with optimized ingestion speed, at the cost of higher memory usage), using
     * a limited number of bins. When the maximum number of bins is reached, bins with lowest indices are collapsed,
     * which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     */
    public static DDSketch fastCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast {@code DDSketch} (with optimized ingestion speed, at the cost of higher memory usage), using
     * a limited number of bins. When the maximum number of bins is reached, bins with highest indices are collapsed,
     * which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     */
    public static DDSketch fastCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal {@code DDSketch} (with optimized memory usage, at the cost of lower ingestion speed).
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     */
    public static DDSketch memoryOptimal(double relativeAccuracy) {
        return new DDSketch(
            new LogarithmicMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a memory-optimal {@code DDSketch} (with optimized memory usage, at the cost of lower ingestion
     * speed), using a limited number of bins. When the maximum number of bins is reached, bins with lowest indices
     * are  collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     */
    public static DDSketch memoryOptimalCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new LogarithmicMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal {@code DDSketch} (with optimized memory usage, at the cost of lower ingestion
     * speed), using a limited number of bins. When the maximum number of bins is reached, bins with highest indices
     * are  collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     */
    public static DDSketch memoryOptimalCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new LogarithmicMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }
}
