/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.mapping.CubicallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.CollapsingHighestDenseStore;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.DenseStore;
import com.datadoghq.sketch.ddsketch.store.SparseStore;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

/**
 * Preset versions of {@link DDSketch}.
 * <p>
 * {@code DDSketch} works by mapping floating-point input values to bins and counting the number of values for each bin.
 * The mapping to bins is handled by an implementation of {@link IndexMapping}, while the underlying structure that
 * keeps track of bin counts is {@link Store}.
 * <p>
 * {@link LogarithmicMapping} is a simple mapping that maps values to their logarithm to a properly chosen base that
 * ensures the relative accuracy. It is also the one that offers the smallest memory footprint under the relative
 * accuracy guarantee of the sketch. However, because the logarithm may be costly to compute, other mappings can be
 * favored, such as the {@link CubicallyInterpolatedMapping}, which computes indexes at a faster rate but requires
 * slightly more memory to ensure the relative accuracy (about 1% more than the {@link LogarithmicMapping}). See {@link
 * IndexMapping} for more details and more mappings.
 * <p>
 * Bin counts are tracked by instances of {@link Store} (one for positive values and another one for negative values).
 * They are essentially objects that map {@code int} indexes to {@code double} counters. Multiple implementations can be
 * used with different behaviors and properties. Implementations of {@link DenseStore} are backed by an array and offer
 * constant-time sketch insertion, but they may waste memory if input values are sparse as they keep track of contiguous
 * bins. {@link SparseStore} only keeps track of non-empty bins, hence a better memory efficiency, but its insertion
 * speed is logarithmic in the number of non-empty bins.
 * <p>
 * As an order of magnitude, when using {@link UnboundedSizeDenseStore} (e.g., {@link #unboundedDense} and {@link
 * #logarithmicUnboundedDense}), the size of the sketch depends on the logarithmic range that is covered by input
 * values. If \(\alpha\) is the relative accuracy of the sketch, the number of bins that are needed to cover positive
 * values between \(a\) and \(b\) is \(\frac{\log b - \log a}{\log \gamma}\) where \(\gamma =
 * \frac{1+\alpha}{1-\alpha}\). Given that bin counters are tracked using an array of {@code double}, each bin takes 8
 * bytes of memory. If the sketch contains negative values, the same method gives the additional memory size required to
 * track them. To that, a constant memory size needs to be added for other data that the sketch maintains.
 * <p>
 * As an example, if working with durations using {@link #unboundedDense} or {@link #logarithmicUnboundedDense}, with a
 * relative accuracy of 2%, about 2kB (275 bins) is needed to cover values between 1 millisecond and 1 minute, and about
 * 6kB (802 bins) to cover values between 1 nanosecond and 1 day.
 * <p>
 * Bounded dense stores (e.g., {@link #collapsingLowestDense}, {@link #collapsingHighestDense}, {@link
 * #logarithmicCollapsingLowestDense} and {@link #logarithmicCollapsingHighestDense}) limit the size of the sketch to
 * approximately {@code 8 * maxNumBins} by collapsing lowest or highest bins, which therefore cause lowest or highest
 * quantiles to be inaccurate. Collapsing happens only when necessary, that is, when the logarithmic range of input
 * values is too large to be fully tracked with the allowed number of bins, which can be determined using the formula
 * above. As shown in <a href="http://www.vldb.org/pvldb/vol12/p2195-masson.pdf">the DDSketch paper</a>, the likelihood
 * of a store collapsing when using the default bound is vanishingly small for most datasets.
 */
public interface DDSketches {

    /**
     * Constructs an instance of {@code DDSketch} that offers constant-time insertion and whose size grows indefinitely
     * to accommodate for the range of input values.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch unboundedDense(double relativeAccuracy) {
        return new DDSketch(
            new CubicallyInterpolatedMapping(relativeAccuracy),
            UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs an instance of {@code DDSketch} that offers constant-time insertion and whose size grows until the
     * maximum number of bins is reached, at which point bins with lowest indices are collapsed, which causes the
     * relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch, for non-collapsed bins
     * @param maxNumBins the maximum number of bins to be tracked
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch collapsingLowestDense(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new CubicallyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs an instance of {@code DDSketch} that offers constant-time insertion and whose size grows until the
     * maximum number of bins is reached, at which point bins with highest indices are collapsed, which causes the
     * relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch, for non-collapsed bins
     * @param maxNumBins the maximum number of bins to be tracked
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch collapsingHighestDense(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new CubicallyInterpolatedMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs an instance of {@code DDSketch} that offers insertion time that is logarithmic in the number of
     * non-empty bins that the sketch contains and whose size grows indefinitely to accommodate for the range of input
     * values. As opposed to {@link #unboundedDense}, this sketch only tracks non-empty bins, hence its smaller memory
     * footprint, especially if input values are sparse.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch sparse(double relativeAccuracy) {
        return new DDSketch(new CubicallyInterpolatedMapping(relativeAccuracy), SparseStore::new);
    }

    /**
     * Constructs an instance of {@code DDSketch} that offers constant-time insertion and whose size grows indefinitely
     * to accommodate for the range of input values.
     * <p>
     * As opposed to {@link #unboundedDense}, it uses an exactly logarithmic mapping, which is more costly.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch logarithmicUnboundedDense(double relativeAccuracy) {
        return new DDSketch(new LogarithmicMapping(relativeAccuracy), UnboundedSizeDenseStore::new);
    }

    /**
     * Constructs an instance of {@code DDSketch} that offers constant-time insertion and whose size grows until the
     * maximum number of bins is reached, at which point bins with lowest indices are collapsed, which causes the
     * relative accuracy guarantee to be lost on lowest quantiles.
     * <p>
     * As opposed to {@link #collapsingLowestDense}, it uses an exactly logarithmic mapping, which is more costly.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch, for non-collapsed bins
     * @param maxNumBins the maximum number of bins to be tracked
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch logarithmicCollapsingLowestDense(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new LogarithmicMapping(relativeAccuracy),
            () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs an instance of {@code DDSketch} that offers constant-time insertion and whose size grows until the
     * maximum number of bins is reached, at which point bins with highest indices are collapsed, which causes the
     * relative accuracy guarantee to be lost on highest.
     * <p>
     * As opposed to {@link #collapsingHighestDense}, it uses an exactly logarithmic mapping, which is more costly.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch, for non-collapsed bins
     * @param maxNumBins the maximum number of bins to be tracked
     * @return an initially empty instance of {@code DDSketch}
     */
    static DDSketch logarithmicCollapsingHighestDense(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
            new LogarithmicMapping(relativeAccuracy),
            () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }
}
