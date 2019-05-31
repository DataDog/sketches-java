package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.util.dataset.Dataset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class AddingSpeed {

    // Note: we could use JMH

    private final long numWarmupIterations = 1;
    private final int maxNumValues = 100_000_000;
    private final long maxNumAddedValues = 100_000;

    private final String datasetId = "pareto";
    private final Dataset dataset = BenchmarkData.DATASETS.get(datasetId);

    @Test
    void benchmarkAdding() {

        System.out.println("sketch,num_values,num_iterations,min,med,max,avg");

        BenchmarkData.SKETCHES.forEach((sketchId, sketchSupplier) -> {

            final double[] allValues = dataset.get(maxNumValues);

            for (int i = 0; i < numWarmupIterations; i++) {
                final QuantileSketch sketch = sketchSupplier.get();
                sketch.acceptAll(allValues);
            }

            System.out.println("done warming up " + sketchId);

            IntStream.iterate(1, n -> n <= maxNumValues, n -> n * 10).forEach(numValues -> {

                final double[] values = Arrays.copyOf(allValues, numValues);

                final Map<String, List<Double>> results = new LinkedHashMap<>();

                final long numIterations = Math.max(10, maxNumAddedValues / numValues);

                System.gc();

                for (int i = 0; i < numIterations; i++) {
                    final QuantileSketch sketch = sketchSupplier.get();
                    final long startNano = System.nanoTime();
                    sketch.acceptAll(values);
                    final long endNano = System.nanoTime();
                    sketch.applyAsDouble(0.5);
                    final double durationNanoPerAddition = (double) (endNano - startNano) / values.length;

                    final String testCase = String.format(
                            "%s,%d,%d",
                            sketchId,
                            numValues,
                            numIterations
                    );
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
