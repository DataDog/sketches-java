package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.datadoghq.sketch.util.accuracy.RankAccuracyTester;

class RankAccuracy extends Accuracy {

    @Override
    AccuracyTester getAccuracyTester(double[] values) {
        return new RankAccuracyTester(values);
    }
}
