package com.datadoghq.sketch.util.accuracy;

public class RankAccuracyTester extends AccuracyTester {

    public RankAccuracyTester(double[] values) {
        super(values);
    }

    @Override
    public double test(double value, double quantile) {

        final int minExpectedRank = (int) Math.floor(quantile * numValues());
        final int maxExpectedRank = (int) Math.ceil(quantile * numValues());

        int searchIndex = binarySearch(value);
        if (searchIndex < 0) {
            searchIndex = -searchIndex - 1;
        }
        int index = searchIndex;
        while (index > 0 && valueAt(index - 1) >= value) {
            index--;
        }
        int minRank = index;
        while (index < numValues() && valueAt(index) <= value) {
            index++;
        }
        int maxRank = index;

        if (maxRank < minExpectedRank) {
            return (double) (minExpectedRank - maxRank) / numValues();
        } else if (minRank > maxExpectedRank) {
            return (double) (minRank - maxExpectedRank) / numValues();
        } else {
            return 0;
        }
    }
}
