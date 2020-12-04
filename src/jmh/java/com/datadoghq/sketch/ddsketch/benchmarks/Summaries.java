package com.datadoghq.sketch.ddsketch.benchmarks;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Summaries extends BuiltSketchState {

    @Benchmark
    public double getCount() {
        return sketch.getCount();
    }
}
