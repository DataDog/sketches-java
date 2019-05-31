package com.datadoghq.sketch.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class Memory {

    private final double numIterations = 10;
    private final int maxNumValues = 100_000_000;

    @Test
    void test() {

        final Map<String, List<Long>> results = new LinkedHashMap<>();

        BenchmarkData.DATASETS.forEach((datasetId, dataset) -> {

            final int maxSize = dataset.getMaxSize();

            IntStream.iterate(1, n -> n <= maxNumValues && n <= maxSize, n -> n * 10).forEach(numValues -> {

                System.out.println(String.format(
                        "Dataset: %s, numValues: %d",
                        datasetId,
                        numValues
                ));

                for (int iteration = 0; iteration < numIterations; iteration++) {

                    final double[] values = dataset.get(numValues);

                    final Map<String, QuantileSketch<?>> sketches = BenchmarkData.SKETCHES
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));

                    sketches.values().forEach(sketch -> Arrays.stream(values).forEach(sketch));

                    sketches.forEach((sketchId, sketch) -> {

                        final String testCase = String.format(
                                "%s,%s,%d",
                                sketchId,
                                datasetId,
                                numValues
                        );

                        results.computeIfAbsent(testCase, key -> new ArrayList<>()).add(sketch.getMemorySizeEstimate());
                    });
                }
            });
        });

        System.out.println();
        System.out.println("Number of iterations: " + numIterations);
        System.out.println();
        System.out.println("sketch,dataset,num_values,num_iterations,min,med,max,avg");
        results.forEach(
                (testCase, list) -> {

                    final long[] sortedMemorySizes = list.stream().mapToLong(q -> q).sorted().toArray();
                    final long avg = (long) list.stream().mapToLong(q -> q).average().getAsDouble();

                    System.out.println(String.format(
                            "%s,%d,%d,%d,%d,%d",
                            testCase,
                            sortedMemorySizes.length,
                            sortedMemorySizes[0],
                            sortedMemorySizes[sortedMemorySizes.length / 2],
                            sortedMemorySizes[sortedMemorySizes.length - 1],
                            avg
                    ));
                }
        );
    }
}
