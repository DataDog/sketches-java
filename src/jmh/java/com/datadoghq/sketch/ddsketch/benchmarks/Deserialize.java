/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.benchmarks;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.encoding.ByteArrayInput;
import com.datadoghq.sketch.ddsketch.encoding.GrowingByteArrayOutput;
import com.google.protobuf.InvalidProtocolBufferException;
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
public class Deserialize extends BuiltSketchState {

  byte[] fromProtoData;
  byte[] decodeData;
  DDSketch decodedSketch;

  @Setup(Level.Trial)
  public void init() throws IOException {
    super.init();
    this.fromProtoData = DDSketchProtoBinding.toProto(sketch).toByteArray();
    final GrowingByteArrayOutput output = new GrowingByteArrayOutput();
    sketch.encode(output, false);
    this.decodeData = output.trimmedCopy();
    this.decodedSketch =
        DDSketch.decode(new ByteArrayInput(decodeData), sketchOption.getStoreSupplier());
  }

  @Benchmark
  public DDSketch fromProto() throws InvalidProtocolBufferException {
    return DDSketchProtoBinding.fromProto(
        sketchOption.getStoreSupplier(),
        com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(fromProtoData));
  }

  @Benchmark
  public DDSketch decode() throws IOException {
    return DDSketch.decode(new ByteArrayInput(decodeData), sketchOption.getStoreSupplier());
  }

  @Benchmark
  public DDSketch decodeReusing() throws IOException {
    decodedSketch.clear();
    decodedSketch.decodeAndMergeWith(new ByteArrayInput(decodeData));
    return decodedSketch;
  }
}
