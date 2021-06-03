/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.mapping;

import static com.datadoghq.sketch.ddsketch.mapping.Interpolation.CUBIC;

/**
 * A fast {@link IndexMapping} that approximates the memory-optimal one (namely {@link
 * LogarithmicMapping}) by extracting the floor value of the logarithm to the base 2 from the binary
 * representations of floating-point values and cubically interpolating the logarithm in-between.
 *
 * <p>Calculating the bucket index with this mapping is much faster than computing the logarithm of
 * the value (by a factor of 6 according to some benchmarks, although it depends on various
 * factors), and this mapping incurs a memory usage overhead of only 1% compared to the
 * memory-optimal {@link LogarithmicMapping}, under the relative accuracy condition. In comparison,
 * the overheads for {@link LinearlyInterpolatedMapping} and {@link
 * QuadraticallyInterpolatedMapping} are respectively 44% and 8%.
 *
 * <p>Here are a few words about how to calculate the optimal polynomial coefficients.
 *
 * <p>The idea is that the exponent of the floating-point representation gives the floor value of
 * the logarithm to base \(2\) of the input value for free. However, we want the logarithm to base
 * \(\gamma = \frac{1+\alpha}{1-\alpha}\), where \(\alpha\) is the relative accuracy of the sketch.
 * We can deduce that from the logarithm to the base \(2\), but that requires more than the floor
 * value to base \(2\), and we need to actually approximate the logarithm between successive powers
 * of \(2\). A way to do that relatively cheaply is to use the significand and to compute operations
 * that are cheap for the CPU such as additions and multiplications. Therefore, writing \(x =
 * 2^e(1+s)\), where \(e\) is an integer and \(0 \leq s \lt 1\), we compute the index as (the floor
 * value of) \(I_{\alpha} = m\frac{\log2}{\log\gamma}(e+P(s))\), where \(P\) is a polynomial (of
 * degree 3 here) and \(m\) is a multiplier (\(\geq1\)) that is large enough to ensure the
 * \(\alpha\)-accuracy of the sketch.
 *
 * <p>We want that multiplier \(m\) to be as low as possible, because the higher \(m\), the smaller
 * the buckets and the more buckets we need to cover the same range of values (hence the larger
 * sketch memory size). But we still need the buckets to be small enough so that values that are
 * distinct by a multiplying factor equal to \(\gamma\) do not end up in the same bucket (otherwise,
 * the sketch cannot be \(\alpha\)-accurate). That is, we want \(I_{\alpha}(\gamma x) -
 * I_{\alpha}(x) \geq 1\), for any \(\alpha\) and its corresponding \(\gamma\) (\(\leq -1\) would
 * work as well). Writing \(f(x) = e + P(s)\), we can show that that condition amounts to \(f\)
 * increasing and \(m \log 2 (f \circ \exp)' \geq 1\) where \(f\) is differentiable (that is not
 * necessarily the case at powers of \(2\)). Therefore, to achieve the best sketch memory
 * efficiency, we need to maximize the infimum of \((f \circ \exp)'\).
 *
 * <p>Given that \(f(2x) = f(x) + 1\), we know that \((f \circ \exp)'(y + \log 2) = (f \circ
 * \exp)'(y)\), and it is enough to study \(f \circ \exp\) between \(0\) and \(\log 2\), that is,
 * with \(\exp y = x = 2^e(1+s)\), for \(e\) equal to \(0\) and \(s\) between \(0\) and \(1\). In
 * other words, we want to find \(P\) that maximizes \(\inf_{y \in [0,\log 2[}(P \circ \exp)'(y)\),
 * which is equal to \(\inf_{s \in [0,1[}P'(s)(1 + s)\).
 *
 * <p>\(f\) is increasing and, it does not have discontinuity points (that would be an
 * underefficient mapping), therefore we can require \(P(0) = 0\) and \(P(1) = 1\). Hence, we can
 * write \(P(s) = s+s(1-s)(u+vs)\) and we end up with only two coefficients \((u,v)\) to optimize.
 * To find the coefficients that maximize the infimum, we can study the variations of \(Q(s) =
 * P'(s)(1+s)\), which is a polynomial of degree \(3\), depending of values of \(u\) and \(v\). We
 * can show that the infimum is maximized if \(u\) and \(v\) are such that the infimum is equal to
 * \(Q(0)\) and \(Q(r)\), where \(r\) is one of the critical point (local minimum) of \(Q\),
 * distinct from \(0\). That gives a quadratic equation in the two variables \(u\) and \(v\), and
 * given that \(Q(0) = 1+u\), we take the solution that maximizes \(u\). Finally, we get \(u = 3/7\)
 * and \(v = -6/35\), or alternatively, \(A\), \(B\) and \(C\) as in the code if we write \(P(s) =
 * As^3+Bs^2+Cs\). You can convince yourself that those are the optimal coefficients by moving the
 * red point on <a href="https://www.desmos.com/calculator/zqcpw4454k">that graph</a>.
 *
 * <p>With those values, we can choose \(m\) as small as \(\frac{7}{10\log 2}\), which is about
 * \(1.01\), hence the memory usage overhead of \(1\%\). For the reverse mapping (getting the value
 * back from the index), implemented as the {@link #value} method, we need to solve a cubic
 * equation, which we can done using <a
 * href="https://en.wikipedia.org/wiki/Cubic_equation#General_cubic_formula">Cardano's formula</a>.
 */
public class CubicallyInterpolatedMapping extends LogLikeIndexMapping {

  // Assuming we write the index as index(v) = floor(multiplier*ln(2)/ln(gamma)*(e+As^3+Bs^2+Cs)),
  // where v=2^e(1+s) and gamma = (1+relativeAccuracy)/(1-relativeAccuracy), those are the
  // coefficients that minimize the multiplier, therefore the memory footprint of the sketch, while
  // ensuring the relative accuracy of the sketch.
  private static final double A = 6.0 / 35.0;
  private static final double B = -3.0 / 5.0;
  private static final double C = 10.0 / 7.0;

  public CubicallyInterpolatedMapping(double relativeAccuracy) {
    super(relativeAccuracy);
  }

  /** {@inheritDoc} */
  CubicallyInterpolatedMapping(double gamma, double indexOffset) {
    super(gamma, indexOffset);
  }

  @Override
  double log(double value) {
    final long longBits = Double.doubleToRawLongBits(value);
    final double s = DoubleBitOperationHelper.getSignificandPlusOne(longBits) - 1;
    final double e = (double) DoubleBitOperationHelper.getExponent(longBits);
    return ((A * s + B) * s + C) * s + e;
  }

  @Override
  double logInverse(double index) {
    final long exponent = (long) Math.floor(index);
    // Derived from Cardano's formula
    final double d0 = B * B - 3 * A * C;
    final double d1 = 2 * B * B * B - 9 * A * B * C - 27 * A * A * (index - exponent);
    final double p = Math.cbrt((d1 - Math.sqrt(d1 * d1 - 4 * d0 * d0 * d0)) / 2);
    final double significandPlusOne = -(B + p + d0 / p) / (3 * A) + 1;
    return DoubleBitOperationHelper.buildDouble(exponent, significandPlusOne);
  }

  @Override
  double base() {
    return 2;
  }

  @Override
  double correctingFactor() {
    return 7 / (10 * Math.log(2));
  }

  @Override
  Interpolation interpolation() {
    return CUBIC;
  }
}
