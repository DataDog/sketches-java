package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class DenseStore implements Store {

    private static final int DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT = 64;
    private static final double DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO = 0.1;

    private final int arrayLengthGrowthIncrement;
    private final int arrayLengthOverhead;

    long[] counts;
    int offset;
    int minIndex;
    int maxIndex;

    DenseStore() {
        this(DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT);
    }

    DenseStore(int arrayLengthGrowthIncrement) {
        this(arrayLengthGrowthIncrement, (int) (arrayLengthGrowthIncrement * DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO));
    }

    DenseStore(int arrayLengthGrowthIncrement, int arrayLengthOverhead) {
        if (arrayLengthGrowthIncrement <= 0 || arrayLengthOverhead < 0) {
            throw new IllegalArgumentException("Invalid array growth parameters");
        }
        this.arrayLengthGrowthIncrement = arrayLengthGrowthIncrement;
        this.arrayLengthOverhead = arrayLengthOverhead;
        this.counts = null;
        this.offset = 0;
        this.minIndex = Integer.MAX_VALUE;
        this.maxIndex = Integer.MIN_VALUE;
    }

    DenseStore(DenseStore store) {
        this.arrayLengthGrowthIncrement = store.arrayLengthGrowthIncrement;
        this.arrayLengthOverhead = store.arrayLengthOverhead;
        this.counts = Arrays.copyOf(store.counts, store.counts.length);
        this.offset = store.offset;
        this.minIndex = store.minIndex;
        this.maxIndex = store.maxIndex;
    }

    void resetCounts() {
        resetCountsBetweenIndices(minIndex, maxIndex);
    }

    void resetCountsBetweenIndices(int fromIndex, int toIndex) {
        resetCountsBetweenArrayIndices(getArrayIndex(fromIndex), getArrayIndex(toIndex));
    }

    void resetCountsBetweenArrayIndices(int fromArrayIndex, int toArrayIndex) {
        for (int arrayIndex = fromArrayIndex; arrayIndex <= toArrayIndex; arrayIndex++) {
            counts[arrayIndex] = 0L;
        }
    }

    void addToCount(int index, long value) {
        counts[index - offset] += value;
    }

    @Override
    public int getMinIndex() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return minIndex;
    }

    @Override
    public int getMaxIndex() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return maxIndex;
    }

    @Override
    public void add(int index) {
        final int normalizedIndex = normalizeIndex(index);
        counts[normalizedIndex - offset]++;
    }

    @Override
    public void add(int index, long count) {
        final int normalizedIndex = normalizeIndex(index);
        counts[normalizedIndex - offset] += count;
    }

    abstract int normalizeIndex(int index);

    abstract void normalizeCounts(int newMinIndex, int newMaxIndex);

    int getArrayIndex(int index) {
        return index - offset;
    }

    int getNewLength(int desiredLength) {
        return ((desiredLength + arrayLengthOverhead - 1) / arrayLengthGrowthIncrement + 1) * arrayLengthGrowthIncrement;
    }

    @Override
    public boolean isEmpty() {
        return maxIndex < minIndex;
    }

    void centerCounts(int newMinIndex, int newMaxIndex) {

        final int middleIndex = newMinIndex + (newMaxIndex - newMinIndex) / 2;
        shiftCountsInArray(offset + counts.length / 2 - middleIndex - 1);
        minIndex = newMinIndex;
        maxIndex = newMaxIndex;
    }

    void extendRange(int index) {

        if (isEmpty()) {
            initialize(index);
        } else if (index < minIndex) {
            extendRange(index, maxIndex);
        } else if (index > maxIndex) {
            extendRange(minIndex, index);
        }
    }

    void extendRange(int newMinIndex, int newMaxIndex) {

        if (getArrayIndex(newMinIndex) >= 0 && getArrayIndex(newMaxIndex) < counts.length) {

            minIndex = newMinIndex;
            maxIndex = newMaxIndex;

        } else {

            // To avoid shifting too often when nearing the capacity of the array,
            // we may grow it before we actually reach the capacity.

            final int desiredLength = newMaxIndex - newMinIndex + 1;
            final int newLength = getNewLength(desiredLength);
            if (newLength > counts.length) {
                counts = Arrays.copyOf(counts, newLength);
            }

            normalizeCounts(newMinIndex, newMaxIndex);
        }
    }

    private void initialize(int index) {

        final int initialLength = getNewLength(1);

        counts = new long[initialLength];
        offset = index - initialLength / 2;
        minIndex = maxIndex = index;
    }

    @Override
    public long getTotalCount() {
        return getTotalCount(minIndex, maxIndex);
    }

    long getTotalCount(int fromIndex, int toIndex) {

        if (isEmpty()) {
            return 0;
        }

        final int fromArrayIndex = Math.max(getArrayIndex(fromIndex), 0);
        final int toArrayIndex = Math.min(getArrayIndex(toIndex), counts.length - 1);

        long totalCount = 0L;
        for (int arrayIndex = fromArrayIndex; arrayIndex <= toArrayIndex; arrayIndex++) {
            totalCount += counts[arrayIndex];
        }

        return totalCount;
    }

    void shiftCountsInArray(int shift) {

        final int minArrayIndex = minIndex - offset;
        final int maxArrayIndex = maxIndex - offset;

        System.arraycopy(counts, minArrayIndex, counts, minArrayIndex + shift, maxArrayIndex - minArrayIndex + 1);

        if (shift > 0) {
            Arrays.fill(counts, minArrayIndex, minArrayIndex + shift, 0);
        } else {
            Arrays.fill(counts, maxArrayIndex + 1 + shift, maxArrayIndex + 1, 0);
        }

        offset -= shift;
    }

    @Override
    public Iterator<Bin> getAscendingBinIterator() {

        return new Iterator<>() {

            private int index = minIndex;

            @Override
            public boolean hasNext() {
                return index <= maxIndex;
            }

            @Override
            public Bin next() {
                return new Bin(index, counts[index++ - offset]);
            }
        };
    }

    @Override
    public Iterator<Bin> getDescendingBinIterator() {

        return new Iterator<>() {

            private int index = maxIndex;

            @Override
            public boolean hasNext() {
                return index >= minIndex;
            }

            @Override
            public Bin next() {
                return new Bin(index, counts[index-- - offset]);
            }
        };
    }

    void copyFromOther(DenseStore store) {
        counts = Arrays.copyOf(store.counts, store.counts.length);
        offset = store.offset;
        minIndex = store.minIndex;
        maxIndex = store.maxIndex;
    }

    public int getCountArrayLength() {
        return counts == null ? 0 : counts.length;
    }
}
