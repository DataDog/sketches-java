package com.datadoghq.sketch.ddsketch.store;

public class CollapsingMinDenseStore extends CollapsingDenseStore {

    public CollapsingMinDenseStore(int maxNumBuckets) {
        super(maxNumBuckets);
    }

    private CollapsingMinDenseStore(CollapsingMinDenseStore store) {
        super(store);
    }

    @Override
    int normalizeIndex(int index) {

        if (index < minIndex) {
            if (isCollapsed) {
                return minIndex;
            } else {
                extendRange(index);
                if (isCollapsed) {
                    return minIndex;
                }
            }
        } else if (index > maxIndex) {
            extendRange(index);
        }

        return index;
    }

    @Override
    void normalizeCounts(int newMinIndex, int newMaxIndex) {

        if (newMaxIndex - newMinIndex + 1 > counts.length) {

            // The range of indices is too wide, buckets of lowest indices need to be collapsed.

            newMinIndex = newMaxIndex - counts.length + 1;

            if (newMinIndex >= maxIndex) {

                // There will be only one non-empty bucket.

                final long totalCount = getTotalCount();
                resetCounts();
                offset = newMinIndex;
                minIndex = newMinIndex;
                counts[0] = totalCount;

            } else {

                final int shift = offset - newMinIndex;

                if (shift < 0) {

                    // Collapse the buckets.
                    final long collapsedCount = getTotalCount(minIndex, newMinIndex - 1);
                    resetCountsBetweenIndices(minIndex, newMinIndex - 1);
                    counts[newMinIndex - offset] += collapsedCount;
                    minIndex = newMinIndex;

                    // Shift the buckets to make room for newMaxIndex.
                    shiftCountsInArray(shift);

                } else {

                    // Shift the buckets to make room for newMinIndex.
                    shiftCountsInArray(shift);
                    minIndex = newMinIndex;
                }
            }

            maxIndex = newMaxIndex;

            isCollapsed = true;

        } else {

            centerCounts(newMinIndex, newMaxIndex);

        }
    }

    @Override
    public Store copy() {
        return new CollapsingMinDenseStore(this);
    }

    @Override
    public void mergeWith(Store store) {
        if (store instanceof CollapsingMinDenseStore) {
            mergeWithDenseStore((CollapsingMinDenseStore) store);
        } else {
            addAll(getDescendingBinIterator());
        }
    }

    private void mergeWithDenseStore(CollapsingMinDenseStore store) {

        if (store.isEmpty()) {
            return;
        }

        if (isEmpty()) {
            copyFromOther(store);
            return;
        }

        if (store.minIndex < minIndex || store.maxIndex > maxIndex) {
            extendRange(Math.min(minIndex, store.minIndex), Math.max(maxIndex, store.maxIndex));
        }

        int index = store.minIndex;
        for (; index < minIndex; index++) {
            counts[minIndex - offset] += store.counts[index - store.offset];
        }
        for (; index <= store.maxIndex; index++) {
            counts[index - offset] += store.counts[index - store.offset];
        }
    }
}
