/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2020 Datadog, Inc.
 */

package com.datadoghq.sketch.gk;

import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.QuantileSketchTest;
import java.util.Arrays;

abstract class GKArrayTest extends QuantileSketchTest<GKArray> {

    abstract double rankAccuracy();

    @Override
    public GKArray newSketch() {
        return new GKArray(rankAccuracy());
    }

    @Override
    protected void assertAccurate(boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {

        final double rankAccuracy = (merged ? 2 : 1) * rankAccuracy();

        // Check the rank accuracy.

        final int minExpectedRank = (int) Math.floor(Math.max(quantile - rankAccuracy, 0) * sortedValues.length);
        final int maxExpectedRank = (int) Math.ceil(Math.min(quantile + rankAccuracy, 1) * sortedValues.length);

        int searchIndex = Arrays.binarySearch(sortedValues, actualQuantileValue);
        if (searchIndex < 0) {
            searchIndex = -searchIndex - 1;
        }
        int index = searchIndex;
        while (index > 0 && sortedValues[index - 1] >= actualQuantileValue) {
            index--;
        }
        int minRank = index;
        while (index < sortedValues.length && sortedValues[index] <= actualQuantileValue) {
            index++;
        }
        int maxRank = index;

        if (maxRank < minExpectedRank || minRank > maxExpectedRank) {
            fail();
        }
    }

    static class GKArrayTest1 extends GKArrayTest {

        @Override
        double rankAccuracy() {
            return 1e-1;
        }
    }

    static class GKArrayTest2 extends GKArrayTest {

        @Override
        double rankAccuracy() {
            return 1e-2;
        }
    }

    static class GKArrayTest3 extends GKArrayTest {

        @Override
        double rankAccuracy() {
            return 1e-3;
        }
    }
}
