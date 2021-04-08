/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.benchmarks;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Iterate extends BuiltSketchState {

  @Benchmark
  public void forEach(Blackhole bh) {
    sketch
        .getPositiveValueStore()
        .forEach(
            (index, value) -> {
              bh.consume(index);
              bh.consume(value);
            });
  }

  @Benchmark
  public void ascendingIterator(Blackhole bh) {
    sketch
        .getPositiveValueStore()
        .getAscendingStream()
        .forEach(
            bin -> {
              // don't consume the bin to give scalarisaton a chance after the lamda is inlined
              bh.consume(bin.getIndex());
              bh.consume(bin.getCount());
            });
  }
}
