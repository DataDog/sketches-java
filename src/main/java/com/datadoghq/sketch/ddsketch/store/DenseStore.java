/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class DenseStore implements Store {

    private static final int DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT = 64;
    private static final double DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO = 0.1;

    private final int arrayLengthGrowthIncrement;
    private final int arrayLengthOverhead;

    double[] counts;
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
            throw new IllegalArgumentException("The array growth parameters are not valid.");
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
        this.counts = store.counts == null ? null : Arrays.copyOf(store.counts, store.counts.length);
        this.offset = store.offset;
        this.minIndex = store.minIndex;
        this.maxIndex = store.maxIndex;
    }

    @Override
    public void add(int index) {
        final int arrayIndex = normalize(index);
        counts[arrayIndex]++;
    }

    @Override
    public void add(int index, double count) {
        if (count < 0) {
            throw new IllegalArgumentException("The count cannot be negative.");
        }
        if (count == 0) {
            return;
        }
        final int arrayIndex = normalize(index);
        counts[arrayIndex] += count;
    }

    @Override
    public void add(Bin bin) {
        if (bin.getCount() == 0) {
            return;
        }
        final int arrayIndex = normalize(bin.getIndex());
        counts[arrayIndex] += bin.getCount();
    }

    /**
     * Normalize the store, if necessary, so that the counter of the specified index can be updated.
     *
     * @param index the index of the counter to be updated
     * @return the {@code counts} array index that matches the counter to be updated
     */
    abstract int normalize(int index);

    /**
     * Adjust the {@code counts}, the {@code offset}, the {@code minIndex} and the {@code maxIndex}, without resizing
     * the {@code counts} array, in order to try making it fit the specified range.
     *
     * @param newMinIndex the minimum index to be stored
     * @param newMaxIndex the maximum index to be stored
     */
    abstract void adjust(int newMinIndex, int newMaxIndex);

    void extendRange(int index) {
        extendRange(index, index);
    }

    void extendRange(int newMinIndex, int newMaxIndex) {

        newMinIndex = Math.min(newMinIndex, minIndex);
        newMaxIndex = Math.max(newMaxIndex, maxIndex);

        if (isEmpty()) {

            final int initialLength = Math.toIntExact(getNewLength(newMinIndex, newMaxIndex));
            counts = new double[initialLength];
            offset = newMinIndex;
            minIndex = newMinIndex;
            maxIndex = newMinIndex;
            adjust(newMinIndex, newMaxIndex);

        } else if (newMinIndex >= offset && newMaxIndex < (long) offset + counts.length) {

            minIndex = newMinIndex;
            maxIndex = newMaxIndex;

        } else {

            // To avoid shifting too often when nearing the capacity of the array,
            // we may grow it before we actually reach the capacity.

            final int newLength = Math.toIntExact(getNewLength(newMinIndex, newMaxIndex));
            if (newLength > counts.length) {
                counts = Arrays.copyOf(counts, newLength);
            }

            adjust(newMinIndex, newMaxIndex);
        }
    }

    void shiftCounts(int shift) {

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

    void centerCounts(int newMinIndex, int newMaxIndex) {

        final int middleIndex = newMinIndex + (newMaxIndex - newMinIndex + 1) / 2;
        shiftCounts(offset + counts.length / 2 - middleIndex);

        minIndex = newMinIndex;
        maxIndex = newMaxIndex;
    }

    void resetCounts() {
        resetCounts(minIndex, maxIndex);
    }

    void resetCounts(int fromIndex, int toIndex) {
        Arrays.fill(counts, fromIndex - offset, toIndex - offset + 1, 0);
    }

    @Override
    public boolean isEmpty() {
        return maxIndex < minIndex;
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

    long getNewLength(int newMinIndex, int newMaxIndex) {
        final long desiredLength = (long) newMaxIndex - newMinIndex + 1;
        return ((desiredLength + arrayLengthOverhead - 1) / arrayLengthGrowthIncrement + 1)
            * arrayLengthGrowthIncrement;
    }

    @Override
    public double getTotalCount() {
        return getTotalCount(minIndex, maxIndex);
    }

    double getTotalCount(int fromIndex, int toIndex) {

        if (isEmpty()) {
            return 0;
        }

        final int fromArrayIndex = Math.max(fromIndex - offset, 0);
        final int toArrayIndex = Math.min(toIndex - offset, counts.length - 1);

        double totalCount = 0;
        for (int arrayIndex = fromArrayIndex; arrayIndex <= toArrayIndex; arrayIndex++) {
            totalCount += counts[arrayIndex];
        }

        return totalCount;
    }

    @Override
    public Stream<Bin> getAscendingStream() {
        if (isEmpty()) {
            return Stream.of();
        }
        return IntStream
            .rangeClosed(minIndex, maxIndex)
            .filter(index -> counts[index - offset] > 0)
            .mapToObj(index -> new Bin(index, counts[index - offset]));
    }

    @Override
    public Stream<Bin> getDescendingStream() {
        if (isEmpty()) {
            return Stream.of();
        }
        return IntStream
            .iterate(maxIndex, index -> index - 1)
            .limit(maxIndex - minIndex + 1)
            .filter(index -> counts[index - offset] > 0)
            .mapToObj(index -> new Bin(index, counts[index - offset]));
    }

    @Override
    public Iterator<Bin> getAscendingIterator() {

        return new Iterator<Bin>() {

            private long index = minIndex;

            @Override
            public boolean hasNext() {
                return index <= maxIndex;
            }

            @Override
            public Bin next() {
                final int nextIndex = (int) index;
                do {
                    index++;
                } while (index <= maxIndex && counts[(int) index - offset] == 0);
                return new Bin(nextIndex, counts[nextIndex - offset]);
            }
        };
    }

    @Override
    public Iterator<Bin> getDescendingIterator() {

        return new Iterator<Bin>() {

            private long index = maxIndex;

            @Override
            public boolean hasNext() {
                return index >= minIndex;
            }

            @Override
            public Bin next() {
                final int nextIndex = (int) index;
                do {
                    index--;
                } while (index >= minIndex && counts[(int) index - offset] == 0);
                return new Bin(nextIndex, counts[nextIndex - offset]);
            }
        };
    }

    @Override
    public com.datadoghq.sketch.ddsketch.proto.Store toProto() {
        final com.datadoghq.sketch.ddsketch.proto.Store.Builder builder =
            com.datadoghq.sketch.ddsketch.proto.Store.newBuilder();
        if (counts != null) {
            builder.setContiguousBinIndexOffset(minIndex);
            for (long index = minIndex; index <= maxIndex; index++) {
                builder.addContiguousBinCounts(counts[(int) index - offset]);
            }
        }
        return builder.build();
    }
}
