package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

abstract class Accuracy {

    private final long numIterations = 10;
    private final double quantiles[] = { 0.1, 0.5, 0.95, 0.99 };
    private final int maxNumValues = 100_000_000;

    abstract AccuracyTester getAccuracyTester(double[] values);

    @Test
    void benchmark() {

        System.out.println("Number of iterations: " + numIterations);
        System.out.println("dataset,num_values,sketch,quantile,min,med,max,avg");

        BenchmarkData.DATASETS.forEach((datasetId, dataset) -> {

            final int maxSize = dataset.getMaxSize();

            IntStream.iterate(1, n -> n <= maxNumValues && n <= maxSize, n -> n * 10).forEach(numValues -> {

                final Map<String, List<Double>> results = new LinkedHashMap<>();

                for (int iteration = 0; iteration < numIterations; iteration++) {

                    final double[] values = dataset.get(numValues);

                    final Map<String, QuantileSketch<?>> sketches = BenchmarkData.SKETCHES
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));

                    sketches.values().forEach(sketch -> Arrays.stream(values).forEach(sketch));

                    final AccuracyTester accuracyTester = getAccuracyTester(values);

                    sketches.forEach((sketchId, sketch) -> {

                        for (final double quantile : quantiles) {

                            final double accuracy = accuracyTester.test(sketch, quantile);

                            final String testCase = String.format(
                                    "%s,%f",
                                    sketchId,
                                    quantile
                            );

                            results.computeIfAbsent(testCase, key -> new ArrayList<>()).add(accuracy);
                        }
                    });
                }

                results.forEach(
                        (testCase, list) -> {

                            final double[] sortedAccuracies = list.stream().mapToDouble(q -> q).sorted().toArray();
                            final double avg = list.stream().mapToDouble(q -> q).average().getAsDouble();

                            System.out.println(String.format(
                                    "%s,%d,%s,%e,%e,%e,%e",
                                    datasetId,
                                    numValues,
                                    testCase,
                                    sortedAccuracies[0],
                                    sortedAccuracies[sortedAccuracies.length / 2],
                                    sortedAccuracies[sortedAccuracies.length - 1],
                                    avg
                            ));

                        }
                );
            });
        });
    }
}
