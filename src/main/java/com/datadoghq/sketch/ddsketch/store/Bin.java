package com.datadoghq.sketch.ddsketch.store;

import java.util.Objects;

public final class Bin {

    private final int index;
    private final long count;

    Bin(int index, long count) {
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
