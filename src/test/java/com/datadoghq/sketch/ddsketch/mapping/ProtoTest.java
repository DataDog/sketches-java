package com.datadoghq.sketch.ddsketch.mapping;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtoTest {

    @ParameterizedTest
    @MethodSource
    void testToProto(IndexMapping mapping, com.datadoghq.sketch.ddsketch.proto.IndexMapping proto) {
        assertEquals(proto, mapping.toProto());
    }

    static Stream<Arguments> testToProto() {
        return Stream.of(
            Arguments.of(
                new LogarithmicMapping(1.01, 2),
                com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                    .setGamma(1.01)
                    .setIndexOffset(2)
                    .setInterpolation(com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.NONE)
                    .build()
            ),
            Arguments.of(
                new LinearlyInterpolatedMapping(1.01, 2),
                com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                    .setGamma(1.01)
                    .setIndexOffset(2)
                    .setInterpolation(com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.LINEAR)
                    .build()
            ),
            Arguments.of(
                new QuadraticallyInterpolatedMapping(1.01, 2),
                com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                    .setGamma(1.01)
                    .setIndexOffset(2)
                    .setInterpolation(com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.QUADRATIC)
                    .build()
            ),
            Arguments.of(
                new CubicallyInterpolatedMapping(1.01, 2),
                com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                    .setGamma(1.01)
                    .setIndexOffset(2)
                    .setInterpolation(com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.CUBIC)
                    .build()
            ),
            Arguments.of(
                new BitwiseLinearlyInterpolatedMapping(1e-2),
                com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                    .setGamma(Math.pow(2, 1.0 / (1 << 6)))
                    .setIndexOffset(64)
                    .setInterpolation(com.datadoghq.sketch.ddsketch.proto.IndexMapping.Interpolation.LINEAR)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource
    void testFromProto(com.datadoghq.sketch.ddsketch.proto.IndexMapping proto, IndexMapping mapping) {
        assertEquals(mapping, IndexMapping.fromProto(proto));
    }

    static Stream<Arguments> testFromProto() {
        return Stream.of(
            Arguments.of(
                com.datadoghq.sketch.ddsketch.proto.IndexMapping.newBuilder()
                    .setGamma(1.01)
                    .build(),
                new LogarithmicMapping(1.01, 0)
            )
        );
    }
}
