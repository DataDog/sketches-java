/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.util.accuracy;

public class RelativeAccuracyTester extends AccuracyTester {

    public RelativeAccuracyTester(double[] values) {
        super(values);
    }

    @Override
    public double test(double value, double quantile) {

        final double lowerQuantileValue = valueAt((int) Math.floor(quantile * (numValues() - 1)));
        final double upperQuantileValue = valueAt((int) Math.ceil(quantile * (numValues() - 1)));

        return compute(lowerQuantileValue, upperQuantileValue, value);
    }

    public static double compute(double expected, double actual) {
        return compute(expected, expected, actual);
    }

    public static double compute(double expectedMin, double expectedMax, double actual) {

        if (expectedMin < 0 || expectedMax < 0 || actual < 0) {
            throw new IllegalArgumentException();
        }

        if ((expectedMin <= actual) && (actual <= expectedMax)) {
            return 0;
        } else if (expectedMin == 0 && expectedMax == 0) {
            return actual == 0 ? 0 : Double.POSITIVE_INFINITY;
        } else if (actual < expectedMin) {
            return (expectedMin - actual) / expectedMin;
        } else {
            return (actual - expectedMax) / expectedMax;
        }
    }
}
