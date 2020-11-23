package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.footprint.Distribution;

import com.datadoghq.sketch.ddsketch.mapping.*;
import com.datadoghq.sketch.ddsketch.store.*;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.datadoghq.sketch.ddsketch.footprint.Distributions.*;
import static org.junit.jupiter.api.Assertions.*;

public class SerializerTest {

    public static Stream<Arguments> sketches() {
        return Stream.of(
                new LogarithmicMapping(0.01),
                new BitwiseLinearlyInterpolatedMapping(0.01),
                new LinearlyInterpolatedMapping(0.01),
                new QuadraticallyInterpolatedMapping(0.01),
                new CubicallyInterpolatedMapping(0.01)
        )
                .flatMap(im -> Stream.<Supplier<Store>>of(
                        SparseStore::new,
                        () -> new CollapsingLowestDenseStore(1000),
                        () -> new CollapsingHighestDenseStore(1000),
                        UnboundedSizeDenseStore::new,
                        PaginatedStore::new
                ).map(s -> (Supplier<DDSketch>) () -> new DDSketch(im, s)))
                .flatMap(supplier -> Stream.of(
                        NORMAL.of(0, 1),
                        NORMAL.of(100, 10),
                        POISSON.of(0.01),
                        POISSON.of(0.99),
                        POINT.of(42),
                        UNIFORM.of(100)
                ).map(dist -> Arguments.of(supplier, dist)));
    }


    @ParameterizedTest
    @MethodSource("sketches")
    public void testProtobufSerialization(Supplier<DDSketch> sketchSupplier, Distribution distribution) throws InvalidProtocolBufferException {
        DDSketch sketch = load(sketchSupplier, distribution);
        ByteBuffer buffer = sketch.serialize();
        Assertions.assertEquals(DDSketchProtoBinding.toProto(sketch).getSerializedSize(), buffer.remaining());

        assertArrayEquals(DDSketchProtoBinding.toProto(sketch).toByteArray(), buffer.array());
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto =
                com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(buffer);

        DDSketch recovered = DDSketchProtoBinding.fromProto(UnboundedSizeDenseStore::new, proto);
        Assertions.assertEquals(sketch.getCount(), recovered.getCount(), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        Assertions.assertEquals(sketch.getIndexMapping().relativeAccuracy(),
                recovered.getIndexMapping().relativeAccuracy(), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        Assertions.assertEquals(sketch.getIndexMapping().relativeAccuracy(),
                recovered.getIndexMapping().relativeAccuracy(), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        if (null != sketch.getPositiveValueStore()) {
            assertEquals(sketch.getPositiveValueStore(), recovered.getPositiveValueStore());
        }
        if (null != sketch.getNegativeValueStore()) {
            assertEquals(sketch.getNegativeValueStore(), recovered.getNegativeValueStore());
        }
    }

    private void assertEquals(Store expected, Store actual) {
        Iterator<Bin> expectedIt = expected.getAscendingIterator();
        Iterator<Bin> actualIt = actual.getAscendingIterator();
        while (expectedIt.hasNext() && actualIt.hasNext()) {
            Bin x = expectedIt.next();
            Bin y = actualIt.next();
            Assertions.assertEquals(x.getIndex(), y.getIndex(), expected.getClass().getName());
            Assertions.assertEquals(x.getCount(), y.getCount(), AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);
        }
        assertFalse(expectedIt.hasNext() || actualIt.hasNext());
    }

    private static DDSketch load(Supplier<DDSketch> sketchSupplier, Distribution distribution) {
        DDSketch sketch = sketchSupplier.get();
        for (int i = 0; i < 10_000; ++i) {
            sketch.accept(distribution.nextValue());
        }
        return sketch;
    }
}
