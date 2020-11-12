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
 * A {@link QuantileSketch} with relative-error guarantees. This sketch computes quantile values with an approximation
 * error that is relative to the actual quantile value. It works with both positive and negative input values.
 * <p>
 * For instance, using {@code SignedDDSketch} with a relative accuracy guarantee set to 1%, if the expected quantile
 * value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value is
 * 1000, the computed quantile value is guaranteed to be between 990 and 1010.
 * <p>
 * {@code SignedDDSketch} works by mapping floating-point input values to bins and counting the number of values for
 * each bin. The mapping to bins is handled by {@link IndexMapping}, while the underlying structure that keeps track of
 * bin counts is {@link Store}. {@link #memoryOptimal} constructs a sketch with a logarithmic index mapping, hence low
 * memory footprint, whereas {@link #fast} and {@link #balanced} offer faster ingestion speeds at the cost of larger
 * memory footprints. The size of the sketch can be upper-bounded by using collapsing stores. For instance, {@link
 * #memoryOptimalCollapsingLowest} is the version of {@code SignedDDSketch} described in the paper, and also implemented
 * in <a href="https://github.com/DataDog/sketches-go/">Go</a> and <a href="https://github.com/DataDog/sketches-py/">Python</a>.
 * It collapses lowest bins when the maximum number of buckets is reached. For using a specific {@link IndexMapping} or
 * a specific implementation of {@link Store}, the constructor can be used ({@link #SignedDDSketch(IndexMapping,
 * Supplier)}).
 * <p>
 * The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the
 * more bins are needed to keep track of the input values. As a rough estimate, if working on durations using {@link
 * #memoryOptimal} with a relative accuracy of 2%, about 2kB (275 bins) are needed to cover values between 1 millisecond
 * and 1 minute, and about 6kB (802 bins) to cover values between 1 nanosecond and 1 day. The number of bins that are
 * maintained can be upper-bounded using collapsing stores (see for example {@link #memoryOptimalCollapsingLowest} and
 * {@link #memoryOptimalCollapsingHighest}).
 * <p>
 * Note that this implementation is not thread-safe.
 */
public class SignedDDSketch implements QuantileSketch<SignedDDSketch> {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexedValue;

    private final Store negativeValueStore;
    private final Store positiveValueStore;
    private double zeroCount;

    private SignedDDSketch(
        IndexMapping indexMapping,
        Store negativeValueStore,
        Store positiveValueStore,
        double zeroCount,
        double minIndexedValue
    ) {
        this.indexMapping = indexMapping;
        this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
        this.maxIndexedValue = indexMapping.maxIndexableValue();
        this.negativeValueStore = negativeValueStore;
        this.positiveValueStore = positiveValueStore;
        this.zeroCount = zeroCount;
    }

    private SignedDDSketch(
        IndexMapping indexMapping,
        Store negativeValueStore,
        Store positiveValueStore,
        double zeroCount
    ) {
        this(indexMapping, negativeValueStore, positiveValueStore, zeroCount, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     */
    public SignedDDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
        this(indexMapping, storeSupplier, storeSupplier, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * suppliers.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param negativeValueStoreSupplier the store constructor for keeping track of added negative values
     * @param positiveValueStoreSupplier the store constructor for keeping track of added positive values
     */
    public SignedDDSketch(
        IndexMapping indexMapping,
        Supplier<Store> negativeValueStoreSupplier,
        Supplier<Store> positiveValueStoreSupplier
    ) {
        this(indexMapping, negativeValueStoreSupplier.get(), positiveValueStoreSupplier.get(), 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param negativeValueStoreSupplier the store constructor for keeping track of added negative values
     * @param positiveValueStoreSupplier the store constructor for keeping track of added positive values
     * @param minIndexedValue the least value that should be distinguished from zero
     */
    public SignedDDSketch(
        IndexMapping indexMapping,
        Supplier<Store> negativeValueStoreSupplier,
        Supplier<Store> positiveValueStoreSupplier,
        double minIndexedValue
    ) {
        this(indexMapping, negativeValueStoreSupplier.get(), positiveValueStoreSupplier.get(), 0, minIndexedValue);
    }

    private SignedDDSketch(SignedDDSketch sketch) {
        this.indexMapping = sketch.indexMapping;
        this.minIndexedValue = sketch.minIndexedValue;
        this.maxIndexedValue = sketch.maxIndexedValue;
        this.negativeValueStore = sketch.negativeValueStore.copy();
        this.positiveValueStore = sketch.positiveValueStore.copy();
        this.zeroCount = sketch.zeroCount;
    }

    public IndexMapping getIndexMapping() {
        return indexMapping;
    }

    public Store getNegativeValueStore() {
        return negativeValueStore;
    }

    public Store getPositiveValueStore() {
        return positiveValueStore;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the value is outside the range that is tracked by the sketch
     */
    @Override
    public void accept(double value) {

        checkValueTrackable(value);

        if (value > minIndexedValue) {
            positiveValueStore.add(indexMapping.index(value));
        } else if (value < -minIndexedValue) {
            negativeValueStore.add(indexMapping.index(-value));
        } else {
            zeroCount++;
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

        if (count < 0) {
            throw new IllegalArgumentException("The count cannot be negative.");
        }

        if (value > minIndexedValue) {
            positiveValueStore.add(indexMapping.index(value), count);
        } else if (value < -minIndexedValue) {
            negativeValueStore.add(indexMapping.index(-value), count);
        } else {
            zeroCount += count;
        }
    }

    private void checkValueTrackable(double value) {
        if (value < -maxIndexedValue || value > maxIndexedValue) {
            throw new IllegalArgumentException("The input value is outside the range that is tracked by the sketch.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the other sketch does not use the same index mapping
     */
    @Override
    public void mergeWith(SignedDDSketch other) {

        if (!indexMapping.equals(other.indexMapping)) {
            throw new IllegalArgumentException(
                "The sketches are not mergeable because they do not use the same index mappings."
            );
        }

        negativeValueStore.mergeWith(other.negativeValueStore);
        positiveValueStore.mergeWith(other.positiveValueStore);
        zeroCount += other.zeroCount;
    }

    @Override
    public SignedDDSketch copy() {
        return new SignedDDSketch(this);
    }

    @Override
    public boolean isEmpty() {
        return zeroCount == 0 && negativeValueStore.isEmpty() && positiveValueStore.isEmpty();
    }

    @Override
    public double getCount() {
        return zeroCount + negativeValueStore.getTotalCount() + positiveValueStore.getTotalCount();
    }

    @Override
    public double getMinValue() {
        if (!negativeValueStore.isEmpty()) {
            return -indexMapping.value(negativeValueStore.getMaxIndex());
        } else if (zeroCount > 0) {
            return 0;
        } else {
            return indexMapping.value(positiveValueStore.getMinIndex());
        }
    }

    @Override
    public double getMaxValue() {
        if (!positiveValueStore.isEmpty()) {
            return indexMapping.value(positiveValueStore.getMaxIndex());
        } else if (zeroCount > 0) {
            return 0;
        } else {
            return -indexMapping.value(negativeValueStore.getMinIndex());
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

        double n = 0;

        Iterator<Bin> negativeBinIterator = negativeValueStore.getDescendingIterator();
        while (negativeBinIterator.hasNext()) {
            Bin bin = negativeBinIterator.next();
            if ((n += bin.getCount()) > rank) {
                return -indexMapping.value(bin.getIndex());
            }
        }

        if ((n += zeroCount) > rank) {
            return 0;
        }

        Iterator<Bin> positiveBinIterator = positiveValueStore.getAscendingIterator();
        while (positiveBinIterator.hasNext()) {
            Bin bin = positiveBinIterator.next();
            if ((n += bin.getCount()) > rank) {
                return indexMapping.value(bin.getIndex());
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * Generates a protobuf representation of this {@code SignedDDSketch}.
     *
     * @return a protobuf representation of this {@code SignedDDSketch}
     */
    public com.datadoghq.sketch.ddsketch.proto.DDSketch toProto() {
        return com.datadoghq.sketch.ddsketch.proto.DDSketch.newBuilder()
            .setPositiveValues(positiveValueStore.toProto())
            .setNegativeValues(negativeValueStore.toProto())
            .setZeroCount(zeroCount)
            .setMapping(indexMapping.toProto())
            .build();
    }

    /**
     * Builds a new instance of {@code SignedDDSketch} based on the provided protobuf representation.
     *
     * @param storeSupplier the constructor of the {@link Store} implementation to be used for encoding bin counters
     * @param proto the protobuf representation of a sketch
     * @return an instance of {@code SignedDDSketch} that matches the protobuf representation
     */
    public static SignedDDSketch fromProto(
        Supplier<? extends Store> storeSupplier,
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto
    ) {
        return new SignedDDSketch(
            IndexMapping.fromProto(proto.getMapping()),
            Store.fromProto(storeSupplier, proto.getNegativeValues()),
            Store.fromProto(storeSupplier, proto.getPositiveValues()),
            proto.getZeroCount()
        );
    }

    // Preset sketches

    /**
     * Constructs a balanced instance of {@code SignedDDSketch}, with high ingestion speed and low memory footprint.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a balanced instance of {@code SignedDDSketch}
     */
    public static SignedDDSketch balanced(double relativeAccuracy) {
        return new SignedDDSketch(
            new CubicallyInterpolatedMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a balanced instance of {@code SignedDDSketch}, with high ingestion speed and low memory footprint,
     * using a limited number of bins. When the maximum number of bins is reached, bins with lowest indices are
     * collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a balanced instance of {@code SignedDDSketch} using a limited number of bins
     */
    public static SignedDDSketch balancedCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new SignedDDSketch(
            new CubicallyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a balanced instance of {@code SignedDDSketch}, with high ingestion speed and low memory footprint,,
     * using a limited number of bins. When the maximum number of bins is reached, bins with highest indices are
     * collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a balanced instance of {@code SignedDDSketch} using a limited number of bins
     */
    public static SignedDDSketch balancedCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new SignedDDSketch(
            new CubicallyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast instance of {@code SignedDDSketch}, with optimized ingestion speed, at the cost of higher
     * memory usage.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a fast instance of {@code SignedDDSketch}
     */
    public static SignedDDSketch fast(double relativeAccuracy) {
        return new SignedDDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a fast instance of {@code SignedDDSketch}, with optimized ingestion speed, at the cost of higher
     * memory usage, using a limited number of bins. When the maximum number of bins is reached, bins with lowest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a fast instance of {@code SignedDDSketch} using a limited number of bins
     */
    public static SignedDDSketch fastCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new SignedDDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast instance of {@code SignedDDSketch}, with optimized ingestion speed, at the cost of higher
     * memory usage, using a limited number of bins. When the maximum number of bins is reached, bins with highest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a fast instance of {@code SignedDDSketch} using a limited number of bins
     */
    public static SignedDDSketch fastCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new SignedDDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code SignedDDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a memory-optimal instance of {@code SignedDDSketch}
     */
    public static SignedDDSketch memoryOptimal(double relativeAccuracy) {
        return new SignedDDSketch(
            new LogarithmicMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code SignedDDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed, using a limited number of bins. When the maximum number of bins is reached, bins with lowest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a memory-optimal instance of {@code SignedDDSketch} using a limited number of bins
     */
    public static SignedDDSketch memoryOptimalCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new SignedDDSketch(
            new LogarithmicMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code SignedDDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed, using a limited number of bins. When the maximum number of bins is reached, bins with highest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a memory-optimal instance of {@code SignedDDSketch} using a limited number of bins
     */
    public static SignedDDSketch memoryOptimalCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new SignedDDSketch(
            new LogarithmicMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }
}
