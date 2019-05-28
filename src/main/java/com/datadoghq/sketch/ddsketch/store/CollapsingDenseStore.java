package com.datadoghq.sketch.ddsketch.store;

abstract class CollapsingDenseStore extends DenseStore {

    private final int maxNumBuckets;

    boolean isCollapsed;

    CollapsingDenseStore(int maxNumBuckets) {
        this.maxNumBuckets = maxNumBuckets;
        this.isCollapsed = false;
    }

    CollapsingDenseStore(CollapsingDenseStore store) {
        super(store);
        this.maxNumBuckets = store.maxNumBuckets;
        this.isCollapsed = store.isCollapsed;
    }

    @Override
    int getNewLength(int desiredLength) {
        return Math.min(super.getNewLength(desiredLength), maxNumBuckets);
    }

    void copyFromOther(CollapsingDenseStore store) {
        super.copyFromOther(store);
        isCollapsed = store.isCollapsed;
    }
}
