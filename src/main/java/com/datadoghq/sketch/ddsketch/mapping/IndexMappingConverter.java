/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.BinAcceptor;
import java.util.Iterator;
import java.util.Objects;

/**
 * An interface for converting bins that have been encoded using an {@link IndexMapping} to bins
 * encoded using another one.
 */
public interface IndexMappingConverter {

  /**
   * Converts bins.
   *
   * @param inBins an ascending iterator, that is, an iterator that returns bins whose indexes are
   *     sorted in ascending order
   * @param outBins a consumer that is fed the converted bins
   * @throws IllegalArgumentException if the provided iterator is not ascending
   */
  void convertAscendingIterator(Iterator<Bin> inBins, BinAcceptor outBins);

  /**
   * Returns a converter that uniformly distributes the count of a bin to the overlapping bins of
   * the new mapping based on the shares of the initial bin that the new bins cover.
   *
   * <p>This conversion method is not the one that minimizes the relative accuracy of the quantiles
   * that are computed from the resulting bins. For instance, transferring the full count of a bin
   * of the initial mapping to the single bin of the new mapping that overlaps {@link
   * IndexMapping#value(int)} of the initial mapping allows computing more accurate quantiles.
   * However, this method produces better-looking histograms and avoids conversion artifacts that
   * would cause empty bins or bins with counts that are excessively high relative to its
   * neighbors'.
   *
   * <p>If \(\alpha_i\) is the relative accuracy of the initial mapping {@code inMapping},
   * \(\alpha_o\) the relative accuracy of the new mapping {@code outMapping}, and assuming that the
   * initial bins are not themselves resulting from a conversion (in which case \(\alpha_i\) needs
   * to be adjusted to be the effective relative accuracy of the initial bins), the effective
   * relative accuracy of the quantiles that are computed from the bins that result from this
   * conversion method is upper-bounded by \(\alpha =
   * \frac{(1+\alpha_i)(1+\alpha_o)}{1-\alpha_i}-1\). If relative accuracies are small, this is
   * approximately \(\alpha \approx 2\alpha_i+\alpha_o\).
   *
   * <p>That is because this conversion method causes an input data point to be spread over the full
   * width of a bin of the initial mapping, hence a multiplicative shift of up to \(\gamma_i =
   * \frac{1+\alpha_i}{1-\alpha_i}\) to the right, and down to \(\frac{1}{\gamma_i}\) to the left.
   * In addition, because of the relative error induced by the new mapping, transferring counts to
   * the new mapping will cause an additional multiplicative shift of up to \(1+\alpha_o\) to the
   * right, and down to \(1-\alpha_o\) to the left. Therefore, the resulting relative error is up to
   * \(\alpha = \gamma_i(1+\alpha_o)-1\) to the right and up to \(\alpha' =
   * 1-\frac{1-\alpha_o}{\gamma_i}\) to the left. Because \(\alpha-\alpha' =
   * \frac{1}{\gamma_i}((\gamma_i^2-1)\alpha_o+(\gamma_i-1)^2) \geq 0\) (given that \(\gamma_i \geq
   * 1\) and \(\alpha_o \geq 0\)), the resulting relative error is upper-bounded by \(\alpha =
   * \gamma_i(1+\alpha_o)-1 = \frac{(1+\alpha_i)(1+\alpha_o)}{1-\alpha_i}-1\).
   *
   * <p>In other words, this conversion method causes a single point to be spread over the full
   * width of a bin of the initial mapping, inducing a relative error up to approximately
   * \(2\alpha_i\). In addition, the allocation of counts to the bins of the new mapping causes a
   * relative error that is up to approximately \(\alpha_o\). Informally, here is what can happen in
   * the worst case:
   *
   * <pre>
   * single input value:                                      x
   * initial mapping:                       -|-------o-------|-------o-------|-------o-------|
   * max (q_1) after bin encoding (1):                               x
   * count spreading over full bin (2):                      [---------------]
   * new mapping:                           |---o---|---o---|---o---|---o---|---o---|---o---|-
   * non-empty bins after conversion (3):                       o       o       o
   * max after conversion:                                                      x
   * </pre>
   *
   * The resulting value at quantile \(1\) (i.e., the maximum value) is shifted by \(\alpha_i\)
   * because of (1), an additional \(\alpha_i\) because of (2) and \(\alpha_o\) because of (3).
   *
   * @return a converter that uniformly distributes the count of a bin to the overlapping bins of
   *     the new mapping depending on the shares of the initial bin that the new bins cover
   */
  static IndexMappingConverter distributingUniformly(
      IndexMapping inMapping, IndexMapping outMapping) {
    Objects.requireNonNull(inMapping);
    Objects.requireNonNull(outMapping);
    return (inBins, outBins) -> {
      Integer inIndex = null;
      int outIndex = Integer.MIN_VALUE;
      double value = 0;
      double outCount = 0;

      while (inBins.hasNext()) {
        final Bin inBin = inBins.next();

        if (inIndex != null && inBin.getIndex() <= inIndex) {
          throw new IllegalArgumentException("The bin iterator is not ascending.");
        }
        inIndex = inBin.getIndex();

        final double inLowerBound = inMapping.lowerBound(inBin.getIndex());
        final double inUpperBound = inMapping.upperBound(inBin.getIndex());

        if (inLowerBound < value) {
          throw new RuntimeException("The input mapping is invalid.");
        }
        value = inLowerBound;

        final int newOutIndex = outMapping.index(value);
        if (newOutIndex < outIndex) {
          throw new RuntimeException("The output mapping is invalid.");
        } else if (newOutIndex > outIndex && outCount != 0) {
          outBins.accept(outIndex, outCount);
          outCount = 0;
        }
        outIndex = newOutIndex;

        // Allocate shares of the count of the current input bin to the overlapping bins of the
        // output mapping whose upper bounds are still within the input bin.
        double outUpperBound;
        while ((outUpperBound = outMapping.upperBound(outIndex)) < inUpperBound) {
          outCount += inBin.getCount() * (outUpperBound - value) / (inUpperBound - inLowerBound);
          value = outUpperBound;
          if (outCount != 0) {
            outBins.accept(outIndex, outCount);
            outCount = 0;
          }
          outIndex++;
        }
        // Allocate the remaining of the count of the current input bin to the rightmost overlapping
        // bin. Do not transfer it to outBins just yet as other input bins may also overlap the
        // output bin of index outIndex (we want to forward the whole resulting count at once).
        outCount += inBin.getCount() * (inUpperBound - value) / (inUpperBound - inLowerBound);
      }

      // No other input bin overlaps the output bin of index outIndex. Forward its count to
      // outBins.
      if (outCount != 0) {
        outBins.accept(outIndex, outCount);
      }
    };
  }
}
