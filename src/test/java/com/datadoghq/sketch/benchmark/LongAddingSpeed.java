package com.datadoghq.sketch.benchmark;

public class LongAddingSpeed {

//    final long[] values = Arrays.stream(DoubleValueGenerator.PARETO.generate(1000))
//            .mapToLong(d -> (long) d)
//            .toArray();

//    @Test
//    public void dDSketchWithBinaryMapping() {
//
//        Supplier<DDSketch> sketchSupplier = () -> new DDSketch(new BitwiseLinearlyInterpolatedMapping(1e-2), RingDenseStore::new);
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
//                for (final long value : values) {
//                    sketch.add((double) value);
//                }
//            }
//
//            final long endMills = System.currentTimeMillis();
//            final double additionDurationNanos = (endMills - startMillis) * 1e6 / (numLoops * values.length);
//            System.out.println(String.format("Addition duration: %gns", additionDurationNanos));
//        }
//    }
//
//    @Test
//    void hDRHistogram() {
//
//        final Supplier<Histogram> sketchSupplier = () -> new Histogram(2);
//
//        final long numAdditions = 100_000_000;
//        final long numLoops = numAdditions / values.length;
//
//        while (true) {
//
//            final Histogram sketch = sketchSupplier.get();
//
//            final long startMillis = System.currentTimeMillis();
//
//            for (long i = 0; i < numLoops; i++) {
//                for (final long value : values) {
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
