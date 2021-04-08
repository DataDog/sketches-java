/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.store;

public class UnboundedSizeDenseStore extends DenseStore {

  public UnboundedSizeDenseStore() {
    super();
  }

  public UnboundedSizeDenseStore(int arrayLengthGrowthIncrement) {
    super(arrayLengthGrowthIncrement);
  }

  public UnboundedSizeDenseStore(int arrayLengthGrowthIncrement, int arrayLengthOverhead) {
    super(arrayLengthGrowthIncrement, arrayLengthOverhead);
  }

  private UnboundedSizeDenseStore(UnboundedSizeDenseStore store) {
    super(store);
  }

  @Override
  int normalize(int index) {

    if (index < minIndex || index > maxIndex) {
      extendRange(index);
    }

    return index - offset;
  }

  @Override
  void adjust(int newMinIndex, int newMaxIndex) {
    centerCounts(newMinIndex, newMaxIndex);
  }

  @Override
  public void mergeWith(Store store) {
    if (store instanceof UnboundedSizeDenseStore) {
      mergeWith((UnboundedSizeDenseStore) store);
    } else {
      super.mergeWith(store);
    }
  }

  private void mergeWith(UnboundedSizeDenseStore store) {

    if (store.isEmpty()) {
      return;
    }

    if (store.minIndex < minIndex || store.maxIndex > maxIndex) {
      extendRange(store.minIndex, store.maxIndex);
    }

    for (int index = store.minIndex; index <= store.maxIndex; index++) {
      counts[index - offset] += store.counts[index - store.offset];
    }
  }

  @Override
  public Store copy() {
    return new UnboundedSizeDenseStore(this);
  }
}
