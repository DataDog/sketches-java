package com.datadoghq.sketch.ddsketch.store;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PaginatedStoreTest extends ExhaustiveStoreTest {


    @Override
    protected Store newStore() {
        return new PaginatedStore();
    }

    @Override
    protected void testExtremeValues() {
        // PaginatedStore is not meant to be used with values that are extremely far from one another as it
        // would allocate an excessively large array.
    }

    @Override
    protected void testMergingExtremeValues() {
        // PaginatedStore is not meant to be used with values that are extremely far from one another as it
        // would allocate an excessively large array.
    }

    public static Stream<Arguments> affineTransformations() {
        return Stream.of(
                Arguments.of(1, 0),
                Arguments.of(127, 1),
                Arguments.of(128, 0),
                Arguments.of(128, 1),
                Arguments.of(129, 0),
                Arguments.of(129, 1),
                Arguments.of(-127, 1),
                Arguments.of(-128, 0),
                Arguments.of(-128, 1),
                Arguments.of(-129, 0),
                Arguments.of(-129, 1)
        );
    }

    @ParameterizedTest
    @MethodSource("affineTransformations")
    public void shouldBeEquivalentToUnboundedSizeDenseStore(int m, int c) {
        Store paginatedStore = newStore();
        UnboundedSizeDenseStore denseStore = new UnboundedSizeDenseStore();
        IntStream.range(0, 1000).map(x -> m * x + c).forEach(x -> {
            paginatedStore.add(x);
            denseStore.add(x);
        });
        Iterator<Bin> pit = paginatedStore.getAscendingIterator();
        Iterator<Bin> dit = denseStore.getAscendingIterator();
        while (pit.hasNext() && dit.hasNext()) {
            assertEquals(dit.next().getIndex(), pit.next().getIndex());
        }
        assertFalse(pit.hasNext());
        assertFalse(dit.hasNext());
    }
}
