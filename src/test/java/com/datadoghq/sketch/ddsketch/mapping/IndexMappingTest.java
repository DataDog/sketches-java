/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.datadoghq.sketch.util.accuracy.RelativeAccuracyTester;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

abstract class IndexMappingTest {

  private static final double EPSILON = AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR;
  private static final Offset<Double> DOUBLE_OFFSET =
      offset(AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR);

  final double minTestedRelativeAccuracy = 1e-8;
  final double maxTestedRelativeAccuracy = 1 - 1e-3;
  final double multiplier = 1 + Math.sqrt(2) * 1e-1;

  abstract IndexMapping getMapping(double relativeAccuracy);

  @Test
  void testAccuracy() {
    for (double relativeAccuracy = maxTestedRelativeAccuracy;
        relativeAccuracy >= minTestedRelativeAccuracy;
        relativeAccuracy *= maxTestedRelativeAccuracy) {
      testAccuracy(getMapping(relativeAccuracy), relativeAccuracy);
    }
  }

  @Test
  void testValidity() {
    final double relativeAccuracy = 1e-2;
    final int minIndex = -50;
    final int maxIndex = 50;

    final IndexMapping mapping = getMapping(relativeAccuracy);
    int index = minIndex;
    double bound = mapping.upperBound(index - 1);
    for (; index <= maxIndex; index++) {
      assertThat(mapping.lowerBound(index)).isCloseTo(bound, DOUBLE_OFFSET);
      assertThat(mapping.value(index)).isGreaterThanOrEqualTo(mapping.lowerBound(index));
      assertThat(mapping.upperBound(index)).isGreaterThanOrEqualTo(mapping.value(index));

      assertThat(mapping.index(mapping.lowerBound(index) - EPSILON)).isLessThan(index);
      assertThat(mapping.index(mapping.lowerBound(index) + EPSILON)).isGreaterThanOrEqualTo(index);
      assertThat(mapping.index(mapping.upperBound(index) - EPSILON)).isLessThanOrEqualTo(index);
      assertThat(mapping.index(mapping.upperBound(index) + EPSILON)).isGreaterThan(index);

      bound = mapping.upperBound(index);
    }
  }

  @Test
  abstract void testProtoRoundTrip();

  void testAccuracy(IndexMapping mapping, double relativeAccuracy) {

    // Assert that the stated relative accuracy of the mapping is less than or equal to the
    // requested one.
    RelativeAccuracyTester.assertAccurate(relativeAccuracy, mapping.relativeAccuracy());

    final double maxRelativeAccuracy = assertRelativelyAccurate(mapping);

    // Handy to check that the actual accuracy is consistent with the claimed one (i.e., not much
    // lower).
    /*
    System.out.println(String.format(
            "Relative accuracy - Requested: %g, claimed: %g, actual: %g",
            relativeAccuracy,
            mapping.relativeAccuracy(),
            maxRelativeAccuracy
    ));
     */
  }

  private static double assertRelativelyAccurate(IndexMapping mapping, double value) {

    final double relativeAccuracy =
        RelativeAccuracyTester.compute(value, mapping.value(mapping.index(value)));

    RelativeAccuracyTester.assertAccurate(mapping.relativeAccuracy(), relativeAccuracy);
    return relativeAccuracy;
  }

  private double assertRelativelyAccurate(IndexMapping mapping) {

    double maxRelativeAccuracy = 0;
    for (double value = mapping.minIndexableValue();
        value < mapping.maxIndexableValue();
        value *= multiplier) {
      maxRelativeAccuracy = Math.max(maxRelativeAccuracy, assertRelativelyAccurate(mapping, value));
    }
    maxRelativeAccuracy =
        Math.max(
            maxRelativeAccuracy, assertRelativelyAccurate(mapping, mapping.maxIndexableValue()));

    return maxRelativeAccuracy;
  }
}
