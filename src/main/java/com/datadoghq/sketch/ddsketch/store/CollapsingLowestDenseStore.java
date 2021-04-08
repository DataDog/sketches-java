/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

public class CollapsingLowestDenseStore extends CollapsingDenseStore {

  public CollapsingLowestDenseStore(int maxNumBins) {
    super(maxNumBins);
  }

  private CollapsingLowestDenseStore(CollapsingLowestDenseStore store) {
    super(store);
  }

  @Override
  int normalize(int index) {

    if (index < minIndex) {
      if (isCollapsed) {
        return 0;
      } else {
        extendRange(index);
        if (isCollapsed) {
          return 0;
        }
      }
    } else if (index > maxIndex) {
      extendRange(index);
    }

    return index - offset;
  }

  @Override
  void adjust(int newMinIndex, int newMaxIndex) {

    if ((long) newMaxIndex - newMinIndex + 1 > counts.length) {

      // The range of indices is too wide, buckets of lowest indices need to be collapsed.

      newMinIndex = newMaxIndex - counts.length + 1;

      if (newMinIndex >= maxIndex) {

        // There will be only one non-empty bucket.

        final double totalCount = getTotalCount();
        resetCounts();
        offset = newMinIndex;
        minIndex = newMinIndex;
        counts[0] = totalCount;

      } else {

        final int shift = offset - newMinIndex;

        if (shift < 0) {

          // Collapse the buckets.
          final double collapsedCount = getTotalCount(minIndex, newMinIndex - 1);
          resetCounts(minIndex, newMinIndex - 1);
          counts[newMinIndex - offset] += collapsedCount;
          minIndex = newMinIndex;

          // Shift the buckets to make room for newMaxIndex.
          shiftCounts(shift);

        } else {

          // Shift the buckets to make room for newMinIndex.
          shiftCounts(shift);
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
    return new CollapsingLowestDenseStore(this);
  }

  @Override
  public void mergeWith(Store store) {
    if (store instanceof CollapsingLowestDenseStore) {
      mergeWith((CollapsingLowestDenseStore) store);
    } else {
      getDescendingStream().forEachOrdered(this::add);
    }
  }

  private void mergeWith(CollapsingLowestDenseStore store) {

    if (store.isEmpty()) {
      return;
    }

    if (store.minIndex < minIndex || store.maxIndex > maxIndex) {
      extendRange(store.minIndex, store.maxIndex);
    }

    int index = store.minIndex;
    for (; index < minIndex && index <= store.maxIndex; index++) {
      counts[0] += store.counts[index - store.offset];
    }
    for (; index < store.maxIndex; index++) {
      counts[index - offset] += store.counts[index - store.offset];
    }
    // This is a separate test so that the comparison in the previous loop is strict (<) and handles
    // store.maxIndex = Integer.MAX_VALUE.
    if (index == store.maxIndex) {
      counts[index - offset] += store.counts[index - store.offset];
    }
  }
}
