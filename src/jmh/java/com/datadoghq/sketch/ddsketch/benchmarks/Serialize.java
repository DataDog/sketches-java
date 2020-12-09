package com.datadoghq.sketch.ddsketch.benchmarks;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Serialize extends BuiltSketchState {

  @Benchmark
  public byte[] serialize() {
    return sketch.serialize().array();
  }

  @Benchmark
  public byte[] toProto() {
    return DDSketchProtoBinding.toProto(sketch).toByteArray();
  }
}
