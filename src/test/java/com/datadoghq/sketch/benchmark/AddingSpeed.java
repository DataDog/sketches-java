package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.CollapsingMinDenseStore;
import com.datadoghq.sketch.util.dataset.Dataset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.HdrHistogram.DoubleHistogram;
import org.junit.jupiter.api.Test;

public class AddingSpeed {

    // Note: we could use JMH

    private final long numWarmupIterations = 1;
    private final long numIterations = 1_000;
    private final int maxNumValues = 100_000_000;
    private final long maxNumAddedValues = 1_000_000;

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
//                    final long startMillis = System.currentTimeMillis();
                    sketch.acceptAll(values);
                    final long endNano = System.nanoTime();
//                    final long endMillis = System.currentTimeMillis();
                    sketch.applyAsDouble(0.5);
                    final double durationNanoPerAddition = (double) (endNano - startNano) / values.length;
//                    final double durationNanoPerAddition = (double) (endMillis - startMillis) * 1e6 / values.length;

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

    private final int numMergingIterations = 10;

    @Test
    @SuppressWarnings("unchecked")
    void benchmarkMerging() {

        System.out.println("Number of iterations: " + numIterations);
        System.out.println("dataset,num_values,sketch,quantile,min,med,max,avg");

        BenchmarkData.SKETCHES.forEach((sketchId, sketchSupplier) -> {

            final double[] allValues1 = dataset.get(maxNumValues);
            final double[] allValues2 = dataset.get(maxNumValues);

            IntStream.iterate(maxNumValues, n -> n > 0, n -> n / 10).forEach(numValues -> {

                final double[] values1 = Arrays.copyOf(allValues1, numValues);
                final double[] values2 = Arrays.copyOf(allValues2, numValues);

                final Map<String, List<Double>> results = new LinkedHashMap<>();

//                for (int i = 0; i < numWarmupIterations; i++) {
//
//                    final QuantileSketch sketch1 = sketchSupplier.get();
//                    final QuantileSketch sketch2 = sketchSupplier.get();
//                    sketch1.acceptAll(values1);
//                    sketch2.acceptAll(values2);
//
//                    sketch1.mergeWith(sketch2);
//                }

                final QuantileSketch<?> warmupSketch1 = sketchSupplier.get();
                final QuantileSketch<?> warmupSketch2 = sketchSupplier.get();
                warmupSketch1.acceptAll(values1);
                warmupSketch2.acceptAll(values2);

                for (int i = 0; i < numIterations; i++) {
                    final int numSketches = 10;

                    final QuantileSketch<?>[] sketches1 = new QuantileSketch[numSketches];
                    final QuantileSketch<?>[] sketches2 = new QuantileSketch[numSketches];
                    for (int j = 0; j < numSketches; j++) {
                        sketches1[j] = sketchSupplier.get();
                        sketches2[j] = sketchSupplier.get();
                        sketches1[j].acceptAll(values1);
                        sketches2[j].acceptAll(values2);
                    }

                    final long numWarmupIterations = 1_000_000 / numValues;

                    for (int j = 0; j < numWarmupIterations; j++) {
                        ((QuantileSketch<Object>) warmupSketch1).mergeWith(warmupSketch2);
                    }
//                    System.out.println("warmup done");

                    final long startNano = System.nanoTime();
                    for (int j = 0; j < numSketches; j++) {
                        ((QuantileSketch<Object>) sketches1[j]).mergeWith(sketches2[j]);
                    }
                    final long endNano = System.nanoTime();

                    final double durationNanoPerAddition = (double) (endNano - startNano) / numSketches;

                    final String testCase = String.format(
                            "%s,%d",
                            sketchId,
                            numValues
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

    void benchmark(Consumer<double[]> r) {

    }

    static abstract class AddBenchmark<S> {

        S sketch;

        abstract void setUp();

        abstract void add(double[] values);

//        abstract void peek();
    }

    Map<String, AddBenchmark<?>> DATA = Map.of(

            "DDSketch",
            new AddBenchmark<DDSketch>() {
                @Override
                void setUp() {
                    sketch = new DDSketch(
                            new LogarithmicMapping(1e-2),
                            () -> new CollapsingMinDenseStore(2048)
                    );
                }

                @Override
                void add(double[] values) {

                }
            }

    );


    public static void main(String[] args) {

        double[] values = IntStream.range(0, 10_000)
                .mapToDouble(i -> ThreadLocalRandom.current().nextDouble())
                .toArray();

        while (true) {

            final DDSketch dd = new DDSketch(
                    new LogarithmicMapping(1e-2),
                    () -> new CollapsingMinDenseStore(2048)
            );
            final DoubleHistogram histogram = new DoubleHistogram(2);
            QuantileSketch s = BenchmarkData.SKETCHES.get("DDSketch").get();

            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                for (double v : values) {
//                    dd.add(v);
                    histogram.recordValue(v);
//                    s.accept(v);
                }
//                s.acceptAll(values);
            }

            long end = System.currentTimeMillis();
            System.out.println((end - start) * 1e6 / (10000*values.length));

        }

    }


//    @Test
//    public void benchmark() {
//
//        final double[] values = DoubleValueGenerator.PARETO.generate(10000);
////        benchmark(() -> new DoubleDDSketch(new BitwiseLinearlyInterpolatedMapping(1e-2), OldDenseStore::new), values);
//        benchmark(() -> new DoubleDDSketch(new BitwiseLinearlyInterpolatedMapping(1e-2), () -> new AltDenseStore(64, 0)), values);
////        benchmark(() -> new DoubleDDSketch(new LogarithmicMapping(1e-2), RingDenseStore::new), values);
////        benchmark(() -> new DoubleHDRHistogram(2), values);
////        ddSketch(() -> new DDSketch(new BitwiseLinearlyInterpolatedMapping(1e-2), RingDenseStore::new), values);
//
//    }
//
//    public void benchmark(Supplier<DoubleQuantileSketch> sketchSupplier, double[] values) {
//
////        final long numAdditions = 100_000_000;
////        final long numLoops = numAdditions / values.length;
//
//
//
//        while (true) {
//            final DoubleQuantileSketch sketch = sketchSupplier.get();
//            final long start = System.nanoTime();
//            for (int i = 0; i < 10000; i++) {
//                for (final double value : values) {
//                    sketch.accept(value);
//                }
//            }
//            final long end = System.nanoTime();
//            System.out.println((end - start)/1e6);
//        }
//
//
//
//
//
////            final long endMills = System.currentTimeMillis();
////            final double additionDurationNanos = (endMills - startMillis) * 1e6 / (numLoops * values.length);
////            System.out.println(String.format("Addition duration: %gns", additionDurationNanos));
////        }
//    }

//    public void benchmark(Supplier<DoubleQuantileSketch> sketchSupplier, double[] values) {
//
////        final long numAdditions = 100_000_000;
////        final long numLoops = numAdditions / values.length;
//
////        while (true) {
//
//        final List<Double> durations = new ArrayList<>();
//
//        final DoubleQuantileSketch sketch = sketchSupplier.get();
//
////            final long startMillis = System.currentTimeMillis();
//
//        for (long i = 0; i < numWarmupIterations; i++) {
//            for (final double value : values) {
//                sketch.accept(value);
//            }
//        }
//
//        for (long i = 0; i < numIterations; i++) {
//            final long start = System.nanoTime();
//            for (final double value : values) {
//                sketch.accept(value);
//            }
//            final long end = System.nanoTime();
//            durations.add((double) (end - start) / values.length);
//        }
//
//
//
//
//
////            final long endMills = System.currentTimeMillis();
////            final double additionDurationNanos = (endMills - startMillis) * 1e6 / (numLoops * values.length);
////            System.out.println(String.format("Addition duration: %gns", additionDurationNanos));
////        }
//    }

//    public double benchmark(Runnable runnable) {
//
////        final long numAdditions = 100_000_000;
////        final long numLoops = numAdditions / values.length;
//
////        while (true) {
//
//        final List<Double> durations = new ArrayList<>();
//
//        final DoubleQuantileSketch sketch = sketchSupplier.get();
//
////            final long startMillis = System.currentTimeMillis();
//
//        for (long i = 0; i < numWarmupIterations; i++) {
//            for (final double value : values) {
//                sketch.accept(value);
//            }
//        }
//
//        for (long i = 0; i < numIterations; i++) {
//            final long start = System.nanoTime();
//            for (final double value : values) {
//                sketch.accept(value);
//            }
//            final long end = System.nanoTime();
//            durations.add((double) (end - start) / values.length);
//        }
//
//
//
//
//
////            final long endMills = System.currentTimeMillis();
////            final double additionDurationNanos = (endMills - startMillis) * 1e6 / (numLoops * values.length);
////            System.out.println(String.format("Addition duration: %gns", additionDurationNanos));
////        }
//    }

    //    @Test
//    public void dDSketchWithBinaryMapping() {
//
//        Supplier<DDSketch> sketchSupplier = () -> new DDSketch(new LogarithmicMapping(1e-2), OldDenseStore::new);
//
//        final long numAdditions = 100_000_000;
//        final long numLoops = numAdditions / values.length;
//
//        while (true) {
//
//            final DDSketch sketch = sketchSupplier.get();
//
//            final long startMillis = System.currentTimeMillis();
//
//            for (long i = 0; i < numLoops; i++) {
//                for (final double value : values) {
//                    sketch.add(value);
//                }
//            }
//
//            final long endMills = System.currentTimeMillis();
//            final double additionDurationNanos = (endMills - startMillis) * 1e6 / (numLoops * values.length);
//            System.out.println(String.format("Addition duration: %gns", additionDurationNanos));
//        }
//    }
//
////    final double[] values = DoubleValueGenerator.PARETO.generate(1000);
//
//    //    @Test
//    public void hDRHistogram() {
//
//        final Supplier<DoubleHistogram> sketchSupplier = () -> new DoubleHistogram(2);
//
//        final long numAdditions = 100_000_000;
//        final long numLoops = numAdditions / values.length;
//
//        while (true) {
//
//            final DoubleHistogram sketch = sketchSupplier.get();
//
//            final long startMillis = System.currentTimeMillis();
//
//            for (long i = 0; i < numLoops; i++) {
//                for (final double value : values) {
//                    sketch.recordValue(value);
//                }
//            }
//
//            final long endMills = System.currentTimeMillis();
//            final double additionDurationNanos = (endMills - startMillis) * 1e6 / (numLoops * values.length);
//            System.out.println(String.format("Addition duration: %gns", additionDurationNanos));
//        }
//    }
}
