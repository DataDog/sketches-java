package com.datadoghq.sketch.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class Memory2 {

    private final double numIterations = 10;
    private final int maxNumValues = 100_000;

    private long usedMemory() {
        System.gc();
        return Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory();
    }

    @Test
    void test() {

        final Map<String, List<Long>> results = new LinkedHashMap<>();

        BenchmarkData.DATASETS.forEach((datasetId, dataset) -> {

            IntStream.iterate(1, n -> n <= maxNumValues, n -> n * 10).forEach(numValues -> {

                System.out.println(String.format(
                        "Dataset: %s, numValues: %d",
                        datasetId,
                        numValues
                ));

                for (int iteration = 0; iteration < numIterations; iteration++) {

                    final double[] values = dataset.get(numValues);

                    BenchmarkData.SKETCHES.forEach((sketchId, sketchSupplier) -> {

                        // This is empirical but gives results that match theoretical expectations using sizes of array.

                        final long before = usedMemory();

                        final QuantileSketch<?> sketch = sketchSupplier.get();
                        for (final double value : values) {
                            sketch.accept(value);
                        }

                        // Compressing data, if relevant.
                        sketch.applyAsDouble(0.5);

                        final long after = usedMemory();

                        // References to (hopefully) prevent earlier garbage collection.
                        final QuantileSketch<?> s = sketchSupplier.get();
                        s.accept(values[0]);
                        s.applyAsDouble(0.5);
                        sketch.applyAsDouble(0.5);

                        final String testCase = String.format(
                                "%s,%s,%d",
                                sketchId,
                                datasetId,
                                numValues
                        );

                        results.computeIfAbsent(testCase, key -> new ArrayList<>()).add(after - before);


                    });
                }
            });
        });

        System.out.println();
        System.out.println("Number of iterations: " + numIterations);
        System.out.println();
        System.out.println("sketch,dataset,num_values,quantile,min,med,max,avg");
        results.forEach(
                (testCase, list) -> {

                    final long[] sortedMemorySizes = list.stream().mapToLong(q -> q).sorted().toArray();
                    final long avg = (long) list.stream().mapToLong(q -> q).average().getAsDouble();

                    System.out.println(String.format(
                            "%s,%d,%d,%d,%d",
                            testCase,
                            sortedMemorySizes[0],
                            sortedMemorySizes[sortedMemorySizes.length / 2],
                            sortedMemorySizes[sortedMemorySizes.length - 1],
                            avg
                    ));

                }
        );
    }

}
