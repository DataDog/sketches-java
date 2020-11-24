package com.datadoghq.sketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.*;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProtobufRegressionTest {

    public static Stream<Arguments> sketches() {
        return Stream.<Supplier<Store>>of(
                        SparseStore::new,
                        () -> new CollapsingLowestDenseStore(1000),
                        () -> new CollapsingHighestDenseStore(1000),
                        UnboundedSizeDenseStore::new,
                        PaginatedStore::new
                ).map(s -> (Supplier<DDSketch>) () -> new DDSketch(new BitwiseLinearlyInterpolatedMapping(0.01), s))
                .map(supplier -> Arguments.of(supplier, IntStream.range(0, 1000).mapToDouble(i -> i * 0.01)));
    }


    @ParameterizedTest
    @MethodSource("sketches")
    public void testProtobufSerialization(Supplier<DDSketch> sketchSupplier, DoubleStream values) throws InvalidProtocolBufferException {
        DDSketch sketch = sketchSupplier.get();
        values.forEach(sketch);
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto =
                com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(sketch.toProto().toByteArray());
        DDSketch recovered = DDSketch.fromProto(UnboundedSizeDenseStore::new, proto);
        String message = "mapping before: " + sketch.getIndexMapping().getClass() + " mapping after: " + recovered.getIndexMapping().getClass();
        assertEquals(sketch.getMinValue(), recovered.getMinValue(), 1e-4, message);
        assertEquals(sketch.getMaxValue(), recovered.getMaxValue(), 1e-4, message);
    }
}
