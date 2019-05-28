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
    int normalizeIndex(int index) {

        if (index < minIndex || index > maxIndex) {
            extendRange(index);
        }

        return index;
    }

    @Override
    void normalizeCounts(int newMinIndex, int newMaxIndex) {
        centerCounts(newMinIndex, newMaxIndex);
    }

    @Override
    public void mergeWith(Store store) {
        if (store instanceof UnboundedSizeDenseStore) {
            mergeWithDenseStore((UnboundedSizeDenseStore) store);
        } else {
            super.mergeWith(store);
        }
    }

    private void mergeWithDenseStore(UnboundedSizeDenseStore store) {

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

        for (int index = store.minIndex; index <= store.maxIndex; index++) {
            counts[index - offset] += store.counts[index - store.offset];
        }
    }

    @Override
    public Store copy() {
        return new UnboundedSizeDenseStore(this);
    }
}
