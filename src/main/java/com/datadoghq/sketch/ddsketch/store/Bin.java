/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

import java.util.Objects;

/**
 * A pair of index and count.
 */
public final class Bin {

    private final int index;
    private final long count;

    /**
     * Constructs a bin.
     *
     * @param index the index of the bin
     * @param count the count of the bin
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public Bin(int index, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("The count cannot be negative.");
        }
        this.index = index;
        this.count = count;
    }

    public int getIndex() {
        return index;
    }

    public long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Bin bin = (Bin) o;
        return index == bin.index &&
            count == bin.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, count);
    }
}
