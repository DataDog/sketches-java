package com.datadoghq.sketch.benchmark;

import com.datadoghq.sketch.ddsketch.store.CollapsingMinDenseStore;
import com.datadoghq.sketch.ddsketch.store.DenseStore;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import com.datadoghq.sketch.gk.GKArray;
import com.datadoghq.sketch.util.dataset.FileDataset;
import com.datadoghq.sketch.util.dataset.SyntheticDataset;
import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.util.dataset.Dataset;
import com.github.stanfordfuturedata.momentsketch.SimpleMomentSketch;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;
import org.HdrHistogram.DoubleHistogram;
import org.apache.commons.math3.distribution.ParetoDistribution;

final class BenchmarkData {

    // The variable contribution to the memory size estimate (e.g., the size of variable-length arrays that hold the data) has to be accurately estimated. The memory overhead depends on the JVM and might be inaccurate but that's less important.

    static final Map<String, Supplier<QuantileSketch<?>>> SKETCHES = Map.of(

            "DDSketch",
            () -> new QuantileSketch<DDSketch>() {

                private final DenseStore store = new CollapsingMinDenseStore(2048);
                private final DDSketch sketch = new DDSketch(
                        new LogarithmicMapping(1e-2),
                        () -> store
                );

                @Override
                public DDSketch get() {
                    return sketch;
                }

                @Override
                public void accept(double value) {
                    sketch.add(value);
                }

                @Override
                public void acceptAll(double... values) {
                    for (final double value : values) {
                        sketch.add(value);
                    }
                }

                @Override
                public double applyAsDouble(double quantile) {
                    return sketch.getValueAtQuantile(quantile);
                }

                @Override
                public long getMemorySizeEstimate() {
                    return 16 + // header
                            20 + 8 * store.getCountArrayLength() + // counts
                            4 + // offset
                            4 + // minIndex
                            4 + // maxIndex
                            4 + // maxNumBuckets
                            1 + // isCollapsed
                            3; // padding
                }

                @Override
                public void mergeWith(QuantileSketch<? extends DDSketch> otherSketch) {
                    sketch.mergeWith(otherSketch.get());
                }

            },

            "DDSketch (fast)",
            () -> new QuantileSketch<DDSketch>() {

                private final DenseStore store = new UnboundedSizeDenseStore();
                private final DDSketch sketch = new DDSketch(
                        new BitwiseLinearlyInterpolatedMapping(1e-2),
                        () -> store
                );

                @Override
                public DDSketch get() {
                    return sketch;
                }

                @Override
                public void accept(double value) {
                    sketch.add(value);
                }

                @Override
                public void acceptAll(double... values) {
                    for (final double value : values) {
                        sketch.add(value);
                    }
                }

                @Override
                public double applyAsDouble(double quantile) {
                    return sketch.getValueAtQuantile(quantile);
                }

                @Override
                public long getMemorySizeEstimate() {
                    return 16 + // header
                            20 + 8 * store.getCountArrayLength() + // counts
                            4 + // offset
                            4 + // minIndex
                            4 + // maxIndex
                            4 + // maxNumBuckets
                            1 + // isCollapsed
                            3; // padding
                }

                @Override
                public void mergeWith(QuantileSketch<? extends DDSketch> otherSketch) {
                    sketch.mergeWith(otherSketch.get());
                }

            },

            "HDRHistogram",
            () -> new QuantileSketch<DoubleHistogram>() {

                private final DoubleHistogram histogram = new DoubleHistogram(2);

                @Override
                public DoubleHistogram get() {
                    return histogram;
                }

                @Override
                public void accept(double value) {
                    histogram.recordValue(value);
                }

                @Override
                public void acceptAll(double... values) {
                    for (final double value : values) {
                        histogram.recordValue(value);
                    }
                }

                @Override
                public double applyAsDouble(double quantile) {
                    return histogram.getValueAtPercentile(quantile * 100);
                }


                @Override
                public void mergeWith(QuantileSketch<? extends DoubleHistogram> otherSketch) {
                    histogram.add(otherSketch.get());
                }

                @Override
                public long getMemorySizeEstimate() {
                    return histogram.getEstimatedFootprintInBytes();
                }
            },

            "Moment sketch (non-compressed)",
            () -> new QuantileSketch<SimpleMomentSketch>() {

                private final int k = 20;
                private final SimpleMomentSketch sketch = new SimpleMomentSketch(k);

                {
                    sketch.setCompressed(false);
                }

                @Override
                public SimpleMomentSketch get() {
                    return sketch;
                }

                @Override
                public void accept(double value) {
                    sketch.add(value);
                }

                @Override
                public void acceptAll(double... values) {
                    for (final double value : values) {
                        sketch.add(value);
                    }
                }

                @Override
                public double applyAsDouble(double quantile) {
                    return sketch.getQuantiles(new double[]{ quantile })[0];
                }

                @Override
                public void mergeWith(QuantileSketch<? extends SimpleMomentSketch> otherSketch) {
                    sketch.merge(otherSketch.get());
                }

                @Override
                public long getMemorySizeEstimate() {
                    return 16 + // header (SimpleMomentSketch)
                            8 + // data
                            1 + // useArcSinh
                            7 + // padding
                            16 + // header (MomentStruct)
                            20 + 8 * k + // power_sums
                            8 + // min
                            8 + // max
                            4; // padding
                }
            },

            "Moment sketch (compressed)",
            () -> new QuantileSketch<SimpleMomentSketch>() {

                private final int k = 20;
                private final SimpleMomentSketch sketch = new SimpleMomentSketch(k);

                {
                    sketch.setCompressed(true);
                }

                @Override
                public SimpleMomentSketch get() {
                    return sketch;
                }

                @Override
                public void accept(double value) {
                    sketch.add(value);
                }

                @Override
                public void acceptAll(double... values) {
                    for (final double value : values) {
                        sketch.add(value);
                    }
                }

                @Override
                public double applyAsDouble(double quantile) {
                    return sketch.getQuantiles(new double[]{ quantile })[0];
                }

                @Override
                public void mergeWith(QuantileSketch<? extends SimpleMomentSketch> otherSketch) {
                    sketch.merge(otherSketch.get());
                }

                @Override
                public long getMemorySizeEstimate() {
                    return 16 + // header (SimpleMomentSketch)
                            8 + // data
                            1 + // useArcSinh
                            7 + // padding
                            16 + // header (MomentStruct)
                            20 + 8 * k + // power_sums
                            8 + // min
                            8 + // max
                            4; // padding
                }
            },

            "GKArray",
            () -> new QuantileSketch<GKArray>() {

                private final double rankAccuracy = 1e-2;
                private final GKArray sketch = new GKArray(rankAccuracy);

                @Override
                public GKArray get() {
                    return sketch;
                }

                @Override
                public void accept(double value) {
                    sketch.add(value);
                }

                @Override
                public void acceptAll(double... values) {
                    for (final double value : values) {
                        sketch.add(value);
                    }
                }

                @Override
                public double applyAsDouble(double quantile) {
                    return sketch.getValueAtQuantile(quantile);
                }

                @Override
                public void mergeWith(QuantileSketch<? extends GKArray> otherSketch) {
                    sketch.mergeWith(otherSketch.get());
                }

                @Override
                public long getMemorySizeEstimate() {
                    return 16 + // header
                            8 + // rankAccuracy
                            16 + 20 + (8 + (16 + 8 + 8 + 8)) * sketch.getNumEntries() + 4 + 4 + // entries
                            20 + 8 * ((int) (1 / rankAccuracy) + 1) + // incoming
                            4 + // incomingIndex
                            8 + // minValue
                            8; // compressedCount
                }
            }
    );

    static final Map<String, Dataset> DATASETS = Map.of(

            "pareto",
            new SyntheticDataset(
                    new DoubleSupplier() {
                        private final ParetoDistribution paretoDistribution = new ParetoDistribution();

                        @Override
                        public double getAsDouble() {
                            return paretoDistribution.sample();
                        }
                    }),

            "trace_durations",
            new FileDataset("data/trace_durations"),

            "power",
            new FileDataset("data/power")
    );
}
