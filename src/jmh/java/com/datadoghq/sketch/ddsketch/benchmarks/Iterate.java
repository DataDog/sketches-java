package com.datadoghq.sketch.ddsketch.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Iterate extends BuiltSketchState {

    @Benchmark
    public void forEach(Blackhole bh) {
        sketch.getPositiveValueStore().forEach((index, value) -> {
            bh.consume(index);
            bh.consume(value);
        });
    }

    @Benchmark
    public void ascendingIterator(Blackhole bh) {
        sketch.getPositiveValueStore()
                .getAscendingStream()
                .forEach(bin -> {
                    // don't consume the bin to give scalarisaton a chance
                    // after the lamda is inlined
                    bh.consume(bin.getIndex());
                    bh.consume(bin.getCount());
                });
    }
}
