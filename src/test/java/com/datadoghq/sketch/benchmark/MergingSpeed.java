package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.util.dataset.SyntheticDataset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.stream.IntStream;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.junit.jupiter.api.Test;

class MergingSpeed {


    private final int maxNumValues = 100_000_000;
    private final int maxNumAddedValues = 100_000_000;

    private final SyntheticDataset dataset = new SyntheticDataset(
            new DoubleSupplier() {
                private final ParetoDistribution paretoDistribution = new ParetoDistribution();

                @Override
                public double getAsDouble() {
                    return paretoDistribution.sample();
                }
            });

    @Test
    @SuppressWarnings("unchecked")
    void benchmarkMerging() {

        System.out.println("sketch,num_values,num_iterations,min,med,max,avg");

        BenchmarkData.SKETCHES.forEach((sketchId, sketchSupplier) -> {

            IntStream.iterate(1, n -> n <= maxNumValues, n -> n * 10).forEach(numValues -> {

                final Map<String, List<Double>> results = new LinkedHashMap<>();

                final int numIterations = Math.min(100_000, Math.max(10, maxNumAddedValues / numValues));

                final QuantileSketch<?>[] sketches1 = new QuantileSketch[numIterations];
                final QuantileSketch<?>[] sketches2 = new QuantileSketch[numIterations];
                for (int j = 0; j < numIterations; j++) {
                    sketches1[j] = sketchSupplier.get();
                    sketches2[j] = sketchSupplier.get();
                    for (int i = 0; i < numValues; i++) {
                        sketches1[j].accept(dataset.get());
                    }
                    for (int i = 0; i < numValues; i++) {
                        sketches2[j].accept(dataset.get());
                    }
                }

                final String testCase = String.format(
                        "%s,%d,%d",
                        sketchId,
                        numValues,
                        numIterations
                );

                for (int j = 0; j < numIterations; j++) {
                    final long startNano = System.nanoTime();
                    ((QuantileSketch<Object>) sketches1[j]).mergeWith(sketches2[j]);
                    final long endNano = System.nanoTime();
                    final double durationNanoPerAddition = (double) (endNano - startNano);
                    results.computeIfAbsent(testCase, key -> new ArrayList<>()).add(durationNanoPerAddition);
                }

                printResults(results);
            });
        });

    }

    void printResults(Map<String, List<Double>> results) {
        results.forEach(
                (testCase, list) -> {

                    final double[] sorted = list.stream().mapToDouble(q -> q).sorted().toArray();
                    final double avg = list.stream().mapToDouble(q -> q).average().getAsDouble();

                    System.out.println(String.format(
                            "%s,%e,%e,%e,%e",
                            testCase,
                            sorted[0],
                            sorted[sorted.length / 2],
                            sorted[sorted.length - 1],
                            avg
                    ));

                }
        );
    }
}
