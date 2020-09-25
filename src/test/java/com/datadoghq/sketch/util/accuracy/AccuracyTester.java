/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.util.accuracy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.DoubleStream;

public abstract class AccuracyTester {

    private static final double DEFAULT_QUANTILE_INCREMENT = 0.01;
    private static final double FLOATING_POINT_ACCEPTABLE_ERROR = 1e-12;

    private final double[] sortedValues;

    AccuracyTester(double[] values) {
        this.sortedValues = Arrays.copyOf(values, values.length);
        Arrays.sort(sortedValues);
    }

    double valueAt(int index) {
        return sortedValues[index];
    }

    int numValues() {
        return sortedValues.length;
    }

    int binarySearch(double value) {
        return Arrays.binarySearch(sortedValues, value);
    }

    public abstract double test(double value, double quantile);

    public double test(DoubleUnaryOperator quantileSketch, double quantile) {
        return test(quantileSketch.applyAsDouble(quantile), quantile);
    }

    public double testAllQuantiles(DoubleUnaryOperator quantileSketch) {
        return testAllQuantiles(quantileSketch, DEFAULT_QUANTILE_INCREMENT);
    }

    public double testAllQuantiles(DoubleUnaryOperator quantileSketch, double quantileIncrement) {

        double maxError = 0;
        for (double quantile = 0; quantile < 1; quantile += quantileIncrement) {
            maxError = Math.max(maxError, test(quantileSketch, quantile));
        }
        return Math.max(maxError, test(quantileSketch, 1)); // explicitly test 1 no matter what the increment is
    }

    public void assertAccurate(double maxExpected, DoubleUnaryOperator quantileSketch) {
        assertAccurate(maxExpected, testAllQuantiles(quantileSketch));
    }

    public void assertAccurate(double maxExpected, DoubleUnaryOperator quantileSketch, double quantileIncrement) {
        assertAccurate(maxExpected, testAllQuantiles(quantileSketch, quantileIncrement));
    }

    public static void assertAccurate(double maxExpected, double actual) {
        assertTrue(actual <= maxExpected + FLOATING_POINT_ACCEPTABLE_ERROR);
    }

    public void assertMinExact(DoubleUnaryOperator quantileSketch) {
        assertMinExact(quantileSketch.applyAsDouble(0));
    }

    public void assertMinExact(double actualValue) {
        assertEquals(sortedValues[0], actualValue);
    }

    public void assertMaxExact(DoubleUnaryOperator quantileSketch) {
        assertMaxExact(quantileSketch.applyAsDouble(1));
    }

    public void assertMaxExact(double actualValue) {
        assertEquals(sortedValues[sortedValues.length - 1], actualValue);
    }
}
