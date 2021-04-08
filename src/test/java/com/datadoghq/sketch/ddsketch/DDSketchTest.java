/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

abstract class DDSketchTest extends QuantileSketchTest<DDSketch> {

    abstract double relativeAccuracy();

    IndexMapping mapping() {
        return new LogarithmicMapping(relativeAccuracy());
    }

    Supplier<Store> storeSupplier() {
        return UnboundedSizeDenseStore::new;
    }


    @Override
    public DDSketch newSketch() {
        return new DDSketch(mapping(), storeSupplier());
    }

    @Override
    protected void assertAccurate(boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {

        final double lowerQuantileValue = sortedValues[(int) Math.floor(quantile * (sortedValues.length - 1))];
        final double upperQuantileValue = sortedValues[(int) Math.ceil(quantile * (sortedValues.length - 1))];

        final double minExpected = lowerQuantileValue > 0 ?
                lowerQuantileValue * (1 - relativeAccuracy()) :
                lowerQuantileValue * (1 + relativeAccuracy());

        final double maxExpected = upperQuantileValue > 0 ?
                upperQuantileValue * (1 + relativeAccuracy()) :
                upperQuantileValue * (1 - relativeAccuracy());

        if (actualQuantileValue < minExpected - AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR ||
            actualQuantileValue > maxExpected + AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR) {
            fail();
        }
    }

    @Test
    void testNegativeConstants() {
        testAdding(0);
        testAdding(-1);
        testAdding(-1, -1, -1);
        testAdding(-10, -10, -10);
        testAdding(IntStream.range(0, 10000).mapToDouble(i -> -2).toArray());
        testAdding(-10, -10, -11, -11, -11);
    }

    @Test
    void testNegativeAndPositiveConstants() {
        testAdding(0);
        testAdding(-1, 1);
        testAdding(-1, -1, -1, 1, 1, 1);
        testAdding(-10, -10, -10, 10, 10, 10);
        testAdding(IntStream.range(0, 20000).mapToDouble(i -> i % 2 == 0 ? 2 : -2).toArray());
        testAdding(-10, -10, -11, -11, -11, 10, 10, 11, 11, 11);
    }

    @Test
    void testWithZeros() {

        testAdding(IntStream.range(0, 100).mapToDouble(i -> 0).toArray());

        testAdding(DoubleStream.concat(
                IntStream.range(0, 10).mapToDouble(i -> 0),
                IntStream.range(-100, 100).mapToDouble(i -> i)
        ).toArray());

        testAdding(DoubleStream.concat(
                IntStream.range(-100, 100).mapToDouble(i -> i),
                IntStream.range(0, 10).mapToDouble(i -> 0)
        ).toArray());
    }

    @Test
    void testWithoutZeros() {
        testAdding(DoubleStream.concat(
                IntStream.range(-100, -1).mapToDouble(i -> i),
                IntStream.range(1, 100).mapToDouble(i -> i)
        ).toArray());
    }

    @Test
    void testNegativeNumbersIncreasingLinearly() {
        testAdding(IntStream.range(-10000, 0).mapToDouble(v -> v).toArray());
    }

    @Test
    void testNegativeAndPositiveNumbersIncreasingLinearly() {
        testAdding(IntStream.range(-10000, 10000).mapToDouble(v -> v).toArray());
    }

    @Test
    void testNegativeNumbersDecreasingLinearly() {
        testAdding(IntStream.range(0, 10000).mapToDouble(v -> - v).toArray());
    }

    @Test
    void testNegativeAndPositiveNumbersDecreasingLinearly() {
        testAdding(IntStream.range(0, 20000).mapToDouble(v -> 10000 - v).toArray());
    }

    @Test
    void testNegativeNumbersIncreasingExponentially() {
        testAdding(IntStream.range(0, 100).mapToDouble(i -> -Math.exp(i)).toArray());
    }

    @Test
    void testNegativeAndPositiveNumbersIncreasingExponentially() {
        testAdding(DoubleStream.concat(
                IntStream.range(0, 100).mapToDouble(i -> -Math.exp(i)),
                IntStream.range(0, 100).mapToDouble(Math::exp)
        ).toArray());
    }

    @Test
    void testNegativeNumbersDecreasingExponentially() {
        testAdding(IntStream.range(0, 100).mapToDouble(i -> -Math.exp(-i)).toArray());
    }

    @Test
    void testNegativeAndPositiveNumbersDecreasingExponentially() {
        testAdding(DoubleStream.concat(
                IntStream.range(0, 100).mapToDouble(i -> -Math.exp(-i)),
                IntStream.range(0, 100).mapToDouble(i -> Math.exp(-i))
        ).toArray());
    }

    @Override
    protected void test(boolean merged, double[] values, DDSketch sketch) {
        assertEncodes(merged, values, sketch);
        try {
            testProtoRoundTrip(merged, values, sketch);
        } catch (InvalidProtocolBufferException e) {
            fail(e);
        }
    }

    void testProtoRoundTrip(boolean merged, double[] values, DDSketch sketch) throws InvalidProtocolBufferException {
        assertEncodes(merged, values, DDSketchProtoBinding.fromProto(storeSupplier(), DDSketchProtoBinding.toProto(sketch)));
        assertEncodes(merged, values, DDSketchProtoBinding.fromProto(storeSupplier(),
                com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(sketch.serialize())));
    }

    static class DDSketchTest1 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-1;
        }
    }

    static class DDSketchTest2 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-2;
        }
    }

    static class DDSketchTest3 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-3;
        }
    }
}
