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
   * conversion method is upper-bounded by \(\alpha = \frac{\gamma-1}{\gamma+1}\) where \(\gamma =
   * \gamma_i^2\gamma_o\), \(\gamma_i = \frac{1+\alpha_i}{1-\alpha_i}\), and \(\gamma_o =
   * \frac{1+\alpha_o}{1-\alpha_o}\). If relative accuracies are small, this is approximately
   * \(\alpha \approx 2\alpha_i+\alpha_o\).
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
