/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.CollapsingHighestDenseStore;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.Store;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A {@link QuantileSketch} with the same relative-error guarantees as {@link DDSketch}, but accepts
 * both negative and positive input values.
 * <p>
 * As the negative value store is reversed, to collapse the lowest values its supplier should
 * provide a {@link CollapsingHighestDenseStore} rather than a {@link CollapsingLowestDenseStore}.
 */
public class SignedDDSketch implements QuantileSketch<SignedDDSketch> {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexedValue;

    private final Store negativeValueStore;
    private final Store positiveValueStore;
    private double zeroCount;

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and
     * {@link Store} supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     */
    public SignedDDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
        this(indexMapping, storeSupplier, storeSupplier, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and
     * {@link Store} suppliers.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param negativeValueStoreSupplier the store constructor for keeping track of added negative values
     * @param positiveValueStoreSupplier the store constructor for keeping track of added positive values
     */
    public SignedDDSketch(
            IndexMapping indexMapping,
            Supplier<Store> negativeValueStoreSupplier,
            Supplier<Store> positiveValueStoreSupplier) {
        this(indexMapping, negativeValueStoreSupplier, positiveValueStoreSupplier, 0);
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
            double minIndexedValue) {
        this.indexMapping = indexMapping;
        this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
        this.maxIndexedValue = indexMapping.maxIndexableValue();
        this.negativeValueStore = negativeValueStoreSupplier.get();
        this.positiveValueStore = positiveValueStoreSupplier.get();
        this.zeroCount = 0;
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

}
