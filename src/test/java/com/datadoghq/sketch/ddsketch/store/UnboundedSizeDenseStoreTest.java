package com.datadoghq.sketch.ddsketch.store;

class UnboundedSizeDenseStoreTest extends ExhaustiveStoreTest {

    @Override
    Store newStore() {
        return new UnboundedSizeDenseStore();
    }
}
