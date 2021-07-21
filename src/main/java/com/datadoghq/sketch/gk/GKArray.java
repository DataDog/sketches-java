/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.gk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An implementation of the <a href="http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf">quantile
 * sketch of Greenwald and Khanna</a>.
 */
public class GKArray<T extends Comparable<T>> {

    private final double rankAccuracy;

    private ArrayList<Entry> entries;
    private final int maxIncomingSize;
    private final ArrayList<T> incoming;
    private long compressedCount;
    private T minValue; // nullable

    public GKArray(double rankAccuracy) {
        this.rankAccuracy = rankAccuracy;
        this.entries = new ArrayList<>();
        this.maxIncomingSize = (int) (1 / rankAccuracy) + 1;
        this.incoming = new ArrayList<>(maxIncomingSize);
        this.minValue = null;
        this.compressedCount = 0;
    }

    private GKArray(GKArray<T> sketch) {
        this.rankAccuracy = sketch.rankAccuracy;
        this.entries = new ArrayList<>(sketch.entries);
        this.maxIncomingSize = sketch.maxIncomingSize;
        this.incoming = new ArrayList<>(sketch.incoming);
        this.compressedCount = sketch.compressedCount;
        this.minValue = sketch.minValue;
    }

    public double getRankAccuracy() {
        return rankAccuracy;
    }

    public void accept(T value) {
        incoming.add(value);
        if (incoming.size() == maxIncomingSize) {
            compress();
        }
    }

    public void accept(T value, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("The count cannot be negative.");
        }
        for (long i = 0; i < count; i++) {
            accept(value);
        }
    }

    public void mergeWith(GKArray<T> other) {

        if (rankAccuracy != other.rankAccuracy) {
            throw new IllegalArgumentException(
                "The sketches are not mergeable because they do not use the same accuracy parameter."
            );
        }

        if (other.isEmpty()) {
            return;
        }

        if (isEmpty()) {
            entries = new ArrayList<>(other.entries);
            incoming.addAll(other.incoming);
            compressedCount = other.compressedCount;
            minValue = other.minValue;
            return;
        }

        other.compressIfNecessary();

        final long spread = (long) (other.rankAccuracy * (other.compressedCount - 1));

        final List<Entry> incomingEntries = new ArrayList<>(other.entries.size() + 1);

        long n;
        if ((n = other.entries.get(0).g + other.entries.get(0).delta - spread - 1) > 0) {
            incomingEntries.add(new Entry(other.minValue, n, 0));
        } else {
            minValue = min(minValue, other.minValue);
        }

        for (int i = 0; i < other.entries.size() - 1; i++) {
            incomingEntries.add(new Entry(
                other.entries.get(i).v,
                other.entries.get(i + 1).g + other.entries.get(i + 1).delta - other.entries.get(i).delta,
                0
            ));
        }

        incomingEntries.add(new Entry(
            other.entries.get(other.entries.size() - 1).v,
            spread + 1,
            0
        ));

        compress(incomingEntries, other.compressedCount);
    }

    public GKArray<T> copy() {
        return new GKArray<>(this);
    }

    public boolean isEmpty() {
        return entries.isEmpty() && incoming.isEmpty();
    }

    public void clear() {
        entries.clear();
        incoming.clear();
        compressedCount = 0;
        minValue = null;
    }

    public double getCount() {
        if (!incoming.isEmpty()) {
            compress();
        }
        return compressedCount;
    }

    public T getMinValue() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        compressIfNecessary();
        return minValue;
    }

    public T getMaxValue() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        compressIfNecessary();
        return entries.get(entries.size() - 1).v;
    }

    public T getValueAtQuantile(double quantile) {

        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("The quantile must be between 0 and 1.");
        }

        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        compressIfNecessary();

        if (quantile == 0) { // TODO why is that necessary?
            return minValue;
        }

        final long rank = (long) (quantile * (compressedCount - 1)) + 1;
        final long spread = (long) (rankAccuracy * (compressedCount - 1));
        long gSum = 0;
        int i;
        for (i = 0; i < entries.size(); i++) {
            gSum += entries.get(i).g;
            if (gSum + entries.get(i).delta > rank + spread) { //TODO +1 ?
                break;
            }
        }

        if (i == 0) {
            return minValue;
        } else {
            return entries.get(i - 1).v;
        }
    }

    private void compressIfNecessary() {
        if (!incoming.isEmpty()) {
            compress();
        }
    }

    private void compress() {
        compress(new ArrayList<>(), 0);
    }

    private void compress(List<Entry> additionalEntries, long additionalCount) {

        incoming.forEach(v -> additionalEntries.add(new Entry(v, 1, 0)));
        additionalEntries.sort(Comparator.nullsLast(Comparator.comparing(e -> e.v, T::compareTo)));

        compressedCount += additionalCount + incoming.size();
        if (!additionalEntries.isEmpty()) {
            minValue = min(minValue, additionalEntries.get(0).v);
        }

        final long removalThreshold = 2 * (long) (rankAccuracy * (compressedCount - 1));
        final ArrayList<Entry> mergedEntries = new ArrayList<>(entries.size() + additionalEntries.size() / 3);

        int i = 0, j = 0;
        while (i < additionalEntries.size() || j < entries.size()) {

            if (i == additionalEntries.size()) {

                if (j + 1 < entries.size() &&
                    entries.get(j).g + entries.get(j + 1).g + entries.get(j + 1).delta <= removalThreshold) {
                    // Removable from sketch.
                    entries.get(j + 1).g += entries.get(j).g;
                } else {
                    mergedEntries.add(entries.get(j));
                }

                j++;

            } else if (j == entries.size()) {

                // Done with sketch; now only considering incoming.
                if (i + 1 < additionalEntries.size() &&
                    additionalEntries.get(i).g + additionalEntries.get(i + 1).g + additionalEntries.get(i + 1).delta
                        <= removalThreshold) {
                    // Removable from incoming.
                    additionalEntries.get(i + 1).g += additionalEntries.get(i).g;
                } else {
                    mergedEntries.add(additionalEntries.get(i));
                }

                i++;

            } else if (additionalEntries.get(i).v.compareTo(entries.get(j).v) < 0) {

                if (additionalEntries.get(i).g + entries.get(j).g + entries.get(j).delta <= removalThreshold) {
                    entries.get(j).g += additionalEntries.get(i).g;
                } else {
                    additionalEntries.get(i).delta =
                        entries.get(j).g + entries.get(j).delta - additionalEntries.get(i).g;
                    mergedEntries.add(additionalEntries.get(i));
                }

                i++;

            } else {

                if (j + 1 < entries.size() &&
                    entries.get(j).g + entries.get(j + 1).g + entries.get(j + 1).delta <= removalThreshold) {
                    // Removable from sketch.
                    entries.get(j + 1).g += entries.get(j).g;
                } else {
                    mergedEntries.add(entries.get(j));
                }

                j++;

            }
        }

        entries = mergedEntries;
        incoming.clear();
    }

    private class Entry {

        private final T v; // nullable
        private long g;
        private long delta;

        private Entry(T v, long g, long delta) {
            this.v = v;
            this.g = g;
            this.delta = delta;
        }
    }

    static <T extends Comparable<T>> T min(T a, T b) {
        if (Comparator.nullsLast(T::compareTo).compare(a, b) < 0) {
            return a;
        } else {
            return b;
        }
    }
}
