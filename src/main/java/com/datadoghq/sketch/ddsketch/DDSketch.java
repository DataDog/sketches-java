/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
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
 * For instance, using {@code DDSketch} with a relative accuracy guarantee set to 1%, if the expected quantile
 * value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value
 * is 1000, the computed quantile value is guaranteed to be between 990 and 1010.
 * <p>
 * {@code DDSketch} works by mapping floating-point input values to bins and counting the number of values for each
 * bin. The mapping to bins is handled by {@link IndexMapping}, while the underlying structure that keeps track of
 * bin counts is {@link Store}. {@link #memoryOptimal} constructs a sketch with a logarithmic index mapping, hence low
 * memory footprint, whereas {@link #fast} and {@link #balanced} offer faster ingestion speeds at the cost of
 * larger memory footprints. The size of the sketch can be upper-bounded by using collapsing stores. For instance,
 * {@link #memoryOptimalCollapsingLowest} is the version of {@code DDSketch} described in the paper, and also
 * implemented in <a href="https://github.com/DataDog/sketches-go/">Go</a>
 * and <a href="https://github.com/DataDog/sketches-py/">Python</a>
 * . It collapses lowest bins when the maximum number of buckets is reached. For using a specific
 * {@link IndexMapping} or a specific implementation of {@link Store}, the constructor can be used
 * ({@link #DDSketch(IndexMapping, Supplier)}).
 * <p>
 * The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the
 * more bins are needed to keep track of the input values. As a rough estimate, if working on durations using
 * {@link #memoryOptimal} with a relative accuracy of 2%, about 2kB (275 bins) are needed to cover values between 1
 * millisecond and 1 minute, and about 6kB (802 bins) to cover values between 1 nanosecond and 1 day. The number of
 * bins that are maintained can be upper-bounded using collapsing stores (see for example
 * {@link #memoryOptimalCollapsingLowest} and {@link #memoryOptimalCollapsingHighest}).
 * <p>
 * Note that this implementation is not thread-safe.
 */
public class DDSketch implements QuantileSketch<DDSketch> {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexedValue;

    private final Store store;
    private double zeroCount;

    private DDSketch(IndexMapping indexMapping, Store store, double zeroCount, double minIndexedValue) {
        this.indexMapping = indexMapping;
        this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
        this.maxIndexedValue = indexMapping.maxIndexableValue();
        this.store = store;
        this.zeroCount = zeroCount;
    }

    private DDSketch(IndexMapping indexMapping, Store store, double zeroCount) {
        this(indexMapping, store, zeroCount, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     * @see #balanced
     * @see #balancedCollapsingLowest
     * @see #balancedCollapsingHighest
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
     * @see #balanced
     * @see #balancedCollapsingLowest
     * @see #balancedCollapsingHighest
     * @see #fast
     * @see #fastCollapsingLowest
     * @see #fastCollapsingHighest
     * @see #memoryOptimal
     * @see #memoryOptimalCollapsingLowest
     * @see #memoryOptimalCollapsingHighest
     */
    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier, double minIndexedValue) {
        this(indexMapping, storeSupplier.get(), 0, minIndexedValue);
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
        accept(value, (double) count);
    }

    /**
     * Adds a value to the sketch with a floating-point {@code count}.
     *
     * @param value the value to be added
     * @param count the weight associated with the value to be added
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public void accept(double value, double count) {

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
    public double getCount() {
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
        final double count = getCount();
        return Arrays.stream(quantiles)
                .map(quantile -> getValueAtQuantile(quantile, count))
                .toArray();
    }

    private double getValueAtQuantile(double quantile, double count) {

        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("The quantile must be between 0 and 1.");
        }

        if (count == 0) {
            throw new NoSuchElementException();
        }

        final double rank = quantile * (count - 1);
        if (rank < zeroCount) {
            return 0;
        }

        Bin bin;
        if (quantile <= 0.5) {

            final Iterator<Bin> binIterator = store.getAscendingIterator();
            double n = zeroCount;
            do {
                bin = binIterator.next();
                n += bin.getCount();
            } while (n <= rank && binIterator.hasNext());

        } else {

            final Iterator<Bin> binIterator = store.getDescendingIterator();
            double n = count;
            do {
                bin = binIterator.next();
                n -= bin.getCount();
            } while (n > rank && binIterator.hasNext());
        }

        return indexMapping.value(bin.getIndex());
    }

    /**
     * Generates a protobuf representation of this {@code DDSketch}.
     *
     * @return a protobuf representation of this {@code DDSketch}
     */
    public com.datadoghq.sketch.ddsketch.proto.DDSketch toProto() {
        return com.datadoghq.sketch.ddsketch.proto.DDSketch.newBuilder()
            .setPositiveValues(store.toProto())
            .setZeroCount(zeroCount)
            .setMapping(indexMapping.toProto())
            .build();
    }

    /**
     * Builds a new instance of {@code DDSketch} based on the provided protobuf representation, assuming it encodes
     * non-negative values only.
     *
     * @param storeSupplier the constructor of the {@link Store} implementation to be used for encoding bin counters
     * @param proto         the protobuf representation of a sketch
     * @return an instance of {@code DDSketch} that matches the protobuf representation
     * @throws IllegalArgumentException if the protobuf representation contains negative values
     */
    public static DDSketch fromProto(
        Supplier<? extends Store> storeSupplier,
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto
    ) {
        if (!isEmpty(proto.getNegativeValues())) {
            throw new IllegalArgumentException(
                "Cannot encode a sketch that contains negative values with DDSketch; use SignedDDSketch instead."
            );
        }
        return new DDSketch(
            IndexMapping.fromProto(proto.getMapping()),
            Store.fromProto(storeSupplier, proto.getPositiveValues()),
            proto.getZeroCount()
        );
    }

    private static boolean isEmpty(com.datadoghq.sketch.ddsketch.proto.Store proto) {
        return proto.getBinCountsMap().values().stream().allMatch(count -> count == 0.0) &&
            proto.getContiguousBinCountsList().stream().allMatch(count -> count == 0.0);
    }

    // Preset sketches

    /**
     * Constructs a balanced instance of {@code DDSketch}, with high ingestion speed and low memory footprint.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a balanced instance of {@code DDSketch}
     */
    public static DDSketch balanced(double relativeAccuracy) {
        return new DDSketch(
                new CubicallyInterpolatedMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a balanced instance of {@code DDSketch}, with high ingestion speed and low memory footprint, using
     * a limited number of bins. When the maximum number of bins is reached, bins with lowest indices are collapsed,
     * which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a balanced instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch balancedCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new CubicallyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a balanced instance of {@code DDSketch}, with high ingestion speed and low memory footprint,, using
     * a limited number of bins. When the maximum number of bins is reached, bins with highest indices are collapsed,
     * which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a balanced instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch balancedCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new CubicallyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast instance of {@code DDSketch}, with optimized ingestion speed, at the cost of higher memory
     * usage.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a fast instance of {@code DDSketch}
     */
    public static DDSketch fast(double relativeAccuracy) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a fast instance of {@code DDSketch}, with optimized ingestion speed, at the cost of higher memory
     * usage, using a limited number of bins. When the maximum number of bins is reached, bins with lowest indices
     * are collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a fast instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch fastCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast instance of {@code DDSketch}, with optimized ingestion speed, at the cost of higher memory
     * usage, using a limited number of bins. When the maximum number of bins is reached, bins with highest indices
     * are collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a fast instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch fastCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code DDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a memory-optimal instance of {@code DDSketch}
     */
    public static DDSketch memoryOptimal(double relativeAccuracy) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code DDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed, using a limited number of bins. When the maximum number of bins is reached, bins with lowest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a memory-optimal instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch memoryOptimalCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code DDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed, using a limited number of bins. When the maximum number of bins is reached, bins with highest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a memory-optimal instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch memoryOptimalCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }
}
