package com.datadoghq.sketch.ddsketch.store;

public class CollapsingMaxDenseStore extends CollapsingDenseStore {

    public CollapsingMaxDenseStore(int maxNumBuckets) {
        super(maxNumBuckets);
    }

    private CollapsingMaxDenseStore(CollapsingMaxDenseStore store) {
        super(store);
    }

    @Override
    int normalizeIndex(int index) {

        if (index > maxIndex) {
            if (isCollapsed) {
                return maxIndex;
            } else {
                extendRange(index);
                if (isCollapsed) {
                    return maxIndex;
                }
            }
        } else if (index < minIndex) {
            extendRange(index);
        }

        return index;
    }

    @Override
    void normalizeCounts(int newMinIndex, int newMaxIndex) {

        if (newMaxIndex - newMinIndex + 1 > counts.length) {

            // The range of indices is too wide, buckets of lowest indices need to be collapsed.

            newMaxIndex = newMinIndex + counts.length - 1;

            if (newMaxIndex <= minIndex) {

                // There will be only one non-empty bucket.

                final long totalCount = getTotalCount();
                resetCounts();
                offset = newMinIndex;
                maxIndex = newMaxIndex;
                counts[counts.length - 1] = totalCount;

            } else {

                final int shift = offset - newMinIndex;

                if (shift > 0) {

                    // Collapse the buckets.
                    final long collapsedCount = getTotalCount(newMaxIndex + 1, maxIndex);
                    resetCountsBetweenIndices(newMaxIndex + 1, maxIndex);
                    counts[newMaxIndex - offset] += collapsedCount;
                    maxIndex = newMaxIndex;

                    // Shift the buckets to make room for newMinIndex.
                    shiftCountsInArray(shift);

                } else {

                    // Shift the buckets to make room for newMaxIndex.
                    shiftCountsInArray(shift);
                    maxIndex = newMaxIndex;
                }
            }

            minIndex = newMinIndex;

            isCollapsed = true;

        } else {

            centerCounts(newMinIndex, newMaxIndex);

        }

    }

    @Override
    public Store copy() {
        return new CollapsingMaxDenseStore(this);
    }

    @Override
    public void mergeWith(Store store) {
        if (store instanceof CollapsingMaxDenseStore) {
            mergeWithDenseStore((CollapsingMaxDenseStore) store);
        } else {
            addAll(getAscendingBinIterator());
        }
    }

    private void mergeWithDenseStore(CollapsingMaxDenseStore store) {

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
        for (; index <= maxIndex; index++) {
            counts[index - offset] += store.counts[index - store.offset];
        }
        for (; index <= store.maxIndex; index++) {
            counts[maxIndex - offset] += store.counts[index - store.offset];
        }
    }
}
