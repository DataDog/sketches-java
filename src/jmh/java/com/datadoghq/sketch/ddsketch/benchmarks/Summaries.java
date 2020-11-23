package com.datadoghq.sketch.ddsketch.benchmarks;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchOption;
import com.datadoghq.sketch.ddsketch.DataGenerator;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Summaries {

    @Param
    DataGenerator generator;

    @Param({"NANOSECONDS", "MICROSECONDS", "MILLISECONDS"})
    TimeUnit unit;

    @Param
    DDSketchOption sketchOption;

    @Param("100000")
    int count;

    @Param({"0.01"})
    double relativeAccuracy;

    DDSketch sketch;

    @Setup(Level.Trial)
    public void init() {
        this.sketch = sketchOption.create(relativeAccuracy);
        for (int i = 0; i < count; ++i) {
            sketch.accept(unit.toNanos(Math.abs(Math.round(generator.nextValue()))));
        }
    }

    @Benchmark
    public double getCount() {
        return sketch.getCount();
    }

    @Benchmark
    public double getMax() {
        return sketch.getMaxValue();
    }

    @Benchmark
    public double getMin() {
        return sketch.getMinValue();
    }

    @Benchmark
    public double getMedian() {
        return sketch.getValueAtQuantile(0.5);
    }

    @Benchmark
    public double getP99() {
        return sketch.getValueAtQuantile(0.99);
    }

}
