package com.datadoghq.sketch.ddsketch.store;

public class PaginatedStoreTest extends ExhaustiveStoreTest {


    @Override
    Store newStore() {
        return new PaginatedStore();
    }

    @Override
    void testExtremeValues() {
        // PaginatedStore is not meant to be used with values that are extremely far from one another as it
        // would allocate an excessively large array.
    }

    @Override
    void testMergingExtremeValues() {
        // PaginatedStore is not meant to be used with values that are extremely far from one another as it
        // would allocate an excessively large array.
    }
}
