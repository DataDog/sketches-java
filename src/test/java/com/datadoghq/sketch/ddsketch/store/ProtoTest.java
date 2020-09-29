package com.datadoghq.sketch.ddsketch.store;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtoTest {

    @ParameterizedTest
    @MethodSource
    void testFromProto(com.datadoghq.sketch.ddsketch.proto.Store proto, Map<Integer, Double> binCounts) {

        final Set<Bin> expectedBins = binCounts.entrySet().stream()
            .filter(entry -> entry.getValue() != 0)
            .map(entry -> new Bin(entry.getKey(), entry.getValue()))
            .collect(Collectors.toSet());

        final Store store = Store.fromProto(SparseStore::new, proto);
        final Set<Bin> actualBins = store.getStream().collect(Collectors.toSet());

        assertEquals(expectedBins, actualBins);
    }

    static Stream<Arguments> testFromProto() {
        final Map<Integer, Double> counts = new HashMap<>();
        counts.put(-2, 3.4);
        counts.put(3, 2.9);

        return Stream.of(
            Arguments.of(
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder().build(),
                Collections.emptyMap()
            ),
            Arguments.of(
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder()
                    .putAllBinCounts(counts)
                    .build(),
                counts
            ),
            Arguments.of(
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder()
                    .setContiguousBinIndexOffset(-3)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(3.4)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(2.9)
                    .build(),
                counts
            ),
            Arguments.of(
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder()
                    .putBinCounts(-2, 3.4)
                    .setContiguousBinIndexOffset(3)
                    .addContiguousBinCounts(2.9)
                    .build(),
                counts
            ),
            Arguments.of(
                com.datadoghq.sketch.ddsketch.proto.Store.newBuilder()
                    .putBinCounts(-2, 3)
                    .putBinCounts(3, 2)
                    .setContiguousBinIndexOffset(-2)
                    .addContiguousBinCounts(0.4)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0)
                    .addContiguousBinCounts(0.9)
                    .build(),
                counts
            )
        );
    }
}
