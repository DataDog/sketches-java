/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.WithExactSummaryStatistics;
import com.datadoghq.sketch.ddsketch.encoding.Flag;
import com.datadoghq.sketch.ddsketch.encoding.Input;
import com.datadoghq.sketch.ddsketch.encoding.Output;
import com.datadoghq.sketch.ddsketch.encoding.VarEncodingHelper;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import java.io.IOException;
import java.util.function.Supplier;

public class DDSketchWithExactSummaryStatistics extends WithExactSummaryStatistics<DDSketch> {

  public DDSketchWithExactSummaryStatistics(Supplier<DDSketch> ddSketchConstructor) {
    super(ddSketchConstructor);
  }

  private DDSketchWithExactSummaryStatistics(
      DDSketch sketch,
      double count,
      double sum,
      double sumCompensation,
      double simpleSum,
      double min,
      double max) {
    super(sketch, count, sum, sumCompensation, simpleSum, min, max);
  }

  @Override
  public DDSketchWithExactSummaryStatistics copy() {
    return new DDSketchWithExactSummaryStatistics(
        sketch().copy(), getCount(), sum(), sumCompensation(), simpleSum(), min(), max());
  }

  public void encode(Output output, boolean omitIndexMapping) throws IOException {
    final double count = getCount();
    if (count != 0) {
      Flag.COUNT.encode(output);
      VarEncodingHelper.encodeVarDouble(output, count);
      Flag.MIN.encode(output);
      output.writeDoubleLE(getMinValue());
      Flag.MAX.encode(output);
      output.writeDoubleLE(getMaxValue());
    }
    final double sum = getSum();
    if (sum != 0) {
      Flag.SUM.encode(output);
      output.writeDoubleLE(sum);
    }
    sketch().encode(output, omitIndexMapping);
  }

  public void decodeAndMergeWith(Input input) throws IOException {
    sketch().decodeAndMergeWith(input, this::decodeSummaryStatistic);
  }

  public static DDSketchWithExactSummaryStatistics decode(
      Input input, Supplier<Store> storeSupplier) throws IOException {
    return decode(input, storeSupplier, null);
  }

  public static DDSketchWithExactSummaryStatistics decode(
      Input input, Supplier<Store> storeSupplier, IndexMapping indexMapping) throws IOException {
    final DecodingState state = new DecodingState();
    final DDSketch sketch =
        DDSketch.decode(input, storeSupplier, indexMapping, state::decodeSummaryStatistic);
    // It is assumed that if the count is encoded, other exact summary statistics are encoded as
    // well, which is the case if encode() is used.
    if (state.count == 0 && !sketch.isEmpty()) {
      throw new IllegalArgumentException("The exact summary statistics are missing.");
    }
    return new DDSketchWithExactSummaryStatistics(
        sketch, state.count, state.sum, 0, state.sum, state.min, state.max);
  }

  private void decodeSummaryStatistic(Input input, Flag flag) throws IOException {
    if (Flag.COUNT.equals(flag)) {
      addToCount(VarEncodingHelper.decodeVarDouble(input));
    } else if (Flag.SUM.equals(flag)) {
      addToSum(input.readDoubleLE());
    } else if (Flag.MIN.equals(flag)) {
      updateMin(input.readDoubleLE());
    } else if (Flag.MAX.equals(flag)) {
      updateMax(input.readDoubleLE());
    } else {
      DDSketch.throwInvalidFlagException(input, flag);
    }
  }

  private static final class DecodingState {
    private double count = 0;
    private double sum = 0;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    private void decodeSummaryStatistic(Input input, Flag flag) throws IOException {
      if (Flag.COUNT.equals(flag)) {
        count += VarEncodingHelper.decodeVarDouble(input);
      } else if (Flag.SUM.equals(flag)) {
        sum += input.readDoubleLE();
      } else if (Flag.MIN.equals(flag)) {
        min = Math.min(min, input.readDoubleLE());
      } else if (Flag.MAX.equals(flag)) {
        max = Math.max(max, input.readDoubleLE());
      } else {
        DDSketch.throwInvalidFlagException(input, flag);
      }
    }
  }
}
