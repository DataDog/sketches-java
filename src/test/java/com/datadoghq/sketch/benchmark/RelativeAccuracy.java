package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.datadoghq.sketch.util.accuracy.RelativeAccuracyTester;

class RelativeAccuracy extends Accuracy {

    @Override
    AccuracyTester getAccuracyTester(double[] values) {
        return new RelativeAccuracyTester(values);
    }
}
