/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
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
 * For instance, using {@code DDSketch} with a relative accuracy guarantee set to 1%, if the expected quantile value is
 * 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value is 1000, the
 * computed quantile value is guaranteed to be between 990 and 1010.
 * <p>
 * {@code DDSketch} works by mapping floating-point input values to bins and counting the number of values for each bin.
 * The mapping to bins is handled by {@link IndexMapping}, while the underlying structure that keeps track of bin counts
 * is {@link Store}. The {@link IndexMapping} and the {@link Store} supplier are provided when constructing the sketch
 * and can be adjusted based on use-case requirements. See {@link DDSketches} for preset sketches and some explanation
 * about the involved tradeoffs.
 * <p>
 * Note that negative values are inverted before being mapped to the store. That means that if you use a store that
 * collapses the lowest (resp., the highest) indexes, it will affect the input values that are the closest to (resp.,
 * the farthest away from) zero.
 * <p>
 * Note that this implementation is not thread-safe.
 */
public class DDSketch implements QuantileSketch<DDSketch> {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexedValue;

    private final Store negativeValueStore;
    private final Store positiveValueStore;
    private double zeroCount;

    private DDSketch(
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

    private DDSketch(
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
     * @see DDSketches
     */
    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
        this(indexMapping, storeSupplier, storeSupplier, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * suppliers.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param negativeValueStoreSupplier the store constructor for keeping track of added negative values
     * @param positiveValueStoreSupplier the store constructor for keeping track of added positive values
     * @see DDSketches
     */
    public DDSketch(
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
     * @see DDSketches
     */
    public DDSketch(
        IndexMapping indexMapping,
        Supplier<Store> negativeValueStoreSupplier,
        Supplier<Store> positiveValueStoreSupplier,
        double minIndexedValue
    ) {
        this(indexMapping, negativeValueStoreSupplier.get(), positiveValueStoreSupplier.get(), 0, minIndexedValue);
    }

    private DDSketch(DDSketch sketch) {
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
    public void mergeWith(DDSketch other) {

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
    public DDSketch copy() {
        return new DDSketch(this);
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
     * Generates a protobuf representation of this {@code DDSketch}.
     *
     * @return a protobuf representation of this {@code DDSketch}
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
     * Builds a new instance of {@code DDSketch} based on the provided protobuf representation.
     *
     * @param storeSupplier the constructor of the {@link Store} implementation to be used for encoding bin counters
     * @param proto the protobuf representation of a sketch
     * @return an instance of {@code DDSketch} that matches the protobuf representation
     */
    public static DDSketch fromProto(
        Supplier<? extends Store> storeSupplier,
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto
    ) {
        return new DDSketch(
            IndexMapping.fromProto(proto.getMapping()),
            Store.fromProto(storeSupplier, proto.getNegativeValues()),
            Store.fromProto(storeSupplier, proto.getPositiveValues()),
            proto.getZeroCount()
        );
    }

    // Preset sketches

    /**
     * @deprecated Use {@link DDSketches#unboundedDense(double)}.
     */
    @Deprecated
    public static DDSketch balanced(double relativeAccuracy) {
        return DDSketches.unboundedDense(relativeAccuracy);
    }

    /**
     * @deprecated Use {@link DDSketches#collapsingLowestDense(double, int)}.
     */
    @Deprecated
    public static DDSketch balancedCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return DDSketches.collapsingLowestDense(relativeAccuracy, maxNumBins);
    }

    /**
     * @deprecated Use {@link DDSketches#collapsingHighestDense(double, int)}.
     */
    @Deprecated
    public static DDSketch balancedCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return DDSketches.collapsingHighestDense(relativeAccuracy, maxNumBins);
    }

    /**
     * @deprecated Use  {@code new DDSketch(new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
     * UnboundedSizeDenseStore::new)}.
     */
    @Deprecated
    public static DDSketch fast(double relativeAccuracy) {
        return new DDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * @deprecated Use  {@code new DDSketch(new BitwiseLinearlyInterpolatedMapping(relativeAccuracy), () -> new
     * CollapsingLowestDenseStore(maxNumBins)}.
     */
    @Deprecated
    public static DDSketch fastCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * @deprecated Use  {@code new DDSketch(new BitwiseLinearlyInterpolatedMapping(relativeAccuracy), () -> new
     * CollapsingHighestDenseStore(maxNumBins))}.
     */
    @Deprecated
    public static DDSketch fastCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * @deprecated Use {@link DDSketches#logarithmicCollapsingLowestDense(double, int)}.
     */
    @Deprecated
    public static DDSketch memoryOptimal(double relativeAccuracy) {
        return DDSketches.logarithmicUnboundedDense(relativeAccuracy);
    }

    /**
     * @deprecated Use {@link DDSketches#logarithmicCollapsingLowestDense(double, int)}.
     */
    @Deprecated
    public static DDSketch memoryOptimalCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return DDSketches.logarithmicCollapsingLowestDense(relativeAccuracy, maxNumBins);
    }

    /**
     * @deprecated Use {@link DDSketches#logarithmicCollapsingHighestDense(double, int)}.
     */
    @Deprecated
    public static DDSketch memoryOptimalCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return DDSketches.logarithmicCollapsingHighestDense(relativeAccuracy, maxNumBins);
    }
}
