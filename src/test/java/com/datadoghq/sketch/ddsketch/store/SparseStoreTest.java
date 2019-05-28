package com.datadoghq.sketch.ddsketch.store;

class SparseStoreTest extends ExhaustiveStoreTest {

    @Override
    Store newStore() {
        return new SparseStore();
    }
}
