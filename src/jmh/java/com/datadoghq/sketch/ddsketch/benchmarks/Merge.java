package com.datadoghq.sketch.ddsketch.benchmarks;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchOption;
import com.datadoghq.sketch.ddsketch.DataGenerator;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Merge {

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

    DDSketch left;
    DDSketch right;

    @Setup(Level.Trial)
    public void init() {
        this.left = sketchOption.create(relativeAccuracy);
        this.right = sketchOption.create(relativeAccuracy);
        for (int i = 0; i < count; ++i) {
            left.accept(unit.toNanos(Math.abs(Math.round(generator.nextValue()))));
            right.accept(unit.toNanos(Math.abs(Math.round(generator.nextValue()))));
        }
    }

    @Benchmark
    public Object merge() {
        DDSketch target = sketchOption.create(relativeAccuracy);
        target.mergeWith(left);
        target.mergeWith(right);
        return target;
    }
}
