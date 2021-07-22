/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.benchmarks;

import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.encoding.GrowingByteArrayOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Serialize extends BuiltSketchState {

  GrowingByteArrayOutput output;

  @Setup(Level.Trial)
  public void init() throws IOException {
    super.init();
    this.output = GrowingByteArrayOutput.withDefaultInitialCapacity();
  }

  @Benchmark
  public byte[] serialize() {
    return sketch.serialize().array();
  }

  @Benchmark
  public byte[] toProto() {
    return DDSketchProtoBinding.toProto(sketch).toByteArray();
  }

  @Benchmark
  public byte[] encode() throws IOException {
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    sketch.encode(output, false);
    return output.trimmedCopy();
  }

  @Benchmark
  public byte[] encodeReusing() throws IOException {
    output.clear();
    sketch.encode(output, false);
    return output.trimmedCopy();
  }
}
