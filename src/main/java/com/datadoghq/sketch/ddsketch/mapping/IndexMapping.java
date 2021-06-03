/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import com.datadoghq.sketch.ddsketch.Serializer;

/**
 * A mapping between {@code double} values and {@code int} values that imposes relative guarantees
 * on the composition of {@link #value} and {@link #index}. Specifically, for any value {@code v}
 * between {@link #minIndexableValue()} and {@link #maxIndexableValue()}, implementations of {@link
 * IndexMapping} must be such that {@code value(index(v))} is close to {@code v} with a relative
 * error that is less than {@link #relativeAccuracy()}.
 *
 * <p>In implementations of {@code IndexMapping}, there generally is a trade-off between the cost of
 * computing the index and the number of indices that are required to cover a given range of values
 * (memory optimality). The most memory-optimal mapping is the {@link LogarithmicMapping}, but it
 * requires the costly evaluation of the logarithm when computing the index. Other mappings can
 * approximate the logarithmic mapping, while being less computationally costly. The following table
 * shows the characteristics of a few implementations of {@code IndexMapping}, highlighting the
 * above-mentioned trade-off.
 *
 * <table border="1" style="width:100%">
 * <tr>
 * <td>Mapping</td>
 * <td>Index usage overhead given actual guarantee</td>
 * <td>Max index usage overhead given requested guarantee</td>
 * <td>Computational cost (rough estimate)</td>
 * </tr>
 * <tr>
 * <td>{@link LogarithmicMapping}</td>
 * <td>\(0\%\) (optimal)</td>
 * <td>\(0\%\) (optimal)</td>
 * <td>\(100\%\) (reference)</td>
 * </tr>
 * <tr>
 * <td>{@link QuarticallyInterpolatedMapping}</td>
 * <td>\(\frac{25}{36\log2}-1 \approx 0.19\%\)</td>
 * <td>\(\frac{25}{36\log2}-1 \approx 0.19\%\)</td>
 * <td>\(\sim 19\%\)</td>
 * </tr>
 * <tr>
 * <td>{@link CubicallyInterpolatedMapping}</td>
 * <td>\(\frac{7}{10\log2}-1 \approx 1.0\%\)</td>
 * <td>\(\frac{7}{10\log2}-1 \approx 1.0\%\)</td>
 * <td>\(\sim 16\%\)</td>
 * </tr>
 * <tr>
 * <td>{@link QuadraticallyInterpolatedMapping}</td>
 * <td>\(\frac{3}{4\log2}-1 \approx 8.2\%\)</td>
 * <td>\(\frac{3}{4\log2}-1 \approx 8.2\%\)</td>
 * <td>\(\sim 14\%\)</td>
 * </tr>
 * <tr>
 * <td>{@link LinearlyInterpolatedMapping}</td>
 * <td>\(\frac{1}{\log2}-1 \approx 44\%\)</td>
 * <td>\(\frac{1}{\log2}-1 \approx 44\%\)</td>
 * <td>\(\sim 12\%\)</td>
 * </tr>
 * <tr>
 * <td>{@link BitwiseLinearlyInterpolatedMapping}</td>
 * <td>\(\frac{1}{\log2}-1 \approx 44\%\)</td>
 * <td>\(\frac{2}{\log2}-1 \approx 189\%\)</td>
 * <td>\(\sim 7\%\)</td>
 * </tr>
 * <caption>Comparison of various implementations of {@code IndexMapping}</caption>
 * </table>
 *
 * <p>{@link CubicallyInterpolatedMapping}, which uses a polynomial of degree 3 to approximate the
 * logarithm, usually is a good compromise as its memory overhead compared to the optimal
 * logarithmic mapping is only 1%, and it is about 6 times faster than the logarithmic mapping.
 * Using a polynomial of higher degree (e.g., {@link QuarticallyInterpolatedMapping}) does not yield
 * a significant gain in memory space efficiency (less than 1%), while it degrades its insertion
 * speed to some extent.
 */
public interface IndexMapping {

  int index(double value);

  double value(int index);

  double relativeAccuracy();

  double minIndexableValue();

  double maxIndexableValue();

  int serializedSize();

  void serialize(Serializer serializer);
}
