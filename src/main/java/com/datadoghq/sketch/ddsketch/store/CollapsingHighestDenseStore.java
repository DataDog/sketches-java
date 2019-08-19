/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

public class CollapsingHighestDenseStore extends CollapsingDenseStore {

    public CollapsingHighestDenseStore(int maxNumBins) {
        super(maxNumBins);
    }

    private CollapsingHighestDenseStore(CollapsingHighestDenseStore store) {
        super(store);
    }

    @Override
    int normalize(int index) {

        if (index > maxIndex) {
            if (isCollapsed) {
                return counts.length - 1;
            } else {
                extendRange(index);
                if (isCollapsed) {
                    return counts.length - 1;
                }
            }
        } else if (index < minIndex) {
            extendRange(index);
        }

        return index - offset;
    }

    @Override
    void adjust(int newMinIndex, int newMaxIndex) {

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
                    resetCounts(newMaxIndex + 1, maxIndex);
                    counts[newMaxIndex - offset] += collapsedCount;
                    maxIndex = newMaxIndex;

                    // Shift the buckets to make room for newMinIndex.
                    shiftCounts(shift);

                } else {

                    // Shift the buckets to make room for newMaxIndex.
                    shiftCounts(shift);
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
        return new CollapsingHighestDenseStore(this);
    }

    @Override
    public void mergeWith(Store store) {
        if (store instanceof CollapsingHighestDenseStore) {
            mergeWith((CollapsingHighestDenseStore) store);
        } else {
            getAscendingStream().forEachOrdered(this::add);
        }
    }

    private void mergeWith(CollapsingHighestDenseStore store) {

        if (store.isEmpty()) {
            return;
        }

        if (store.minIndex < minIndex || store.maxIndex > maxIndex) {
            extendRange(store.minIndex, store.maxIndex);
        }

        int index = store.maxIndex;
        for (; index > maxIndex && index >= store.minIndex; index--) {
            counts[counts.length - 1] += store.counts[index - store.offset];
        }
        for (; index >= store.minIndex; index--) {
            counts[index - offset] += store.counts[index - store.offset];
        }
    }
}
