/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch.footprint;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public enum Distributions {
  POINT {
    @Override
    protected Distribution create(double... parameters) {
      return () -> parameters[0];
    }
  },
  UNIFORM {
    @Override
    protected Distribution create(double... parameters) {
      return () -> parameters[0] * ThreadLocalRandom.current().nextDouble();
    }
  },
  NORMAL {
    @Override
    protected Distribution create(double... parameters) {
      return () -> parameters[1] * ThreadLocalRandom.current().nextGaussian() + parameters[0];
    }
  },
  POISSON {
    @Override
    protected Distribution create(double... parameters) {
      return () -> -(Math.log(ThreadLocalRandom.current().nextDouble()) / parameters[0]);
    }
  };

  private static final class NamedDistribution implements Distribution {
    private final Distribution delegate;
    private final Distributions name;
    private final double[] params;

    private NamedDistribution(Distribution delegate, Distributions name, double[] params) {
      this.delegate = delegate;
      this.name = name;
      this.params = params;
    }

    @Override
    public String toString() {
      return name + Arrays.toString(params);
    }

    @Override
    public double nextValue() {
      return delegate.nextValue();
    }
  }

  public Distribution of(double... parameters) {
    return new NamedDistribution(create(parameters), this, parameters);
  }

  protected abstract Distribution create(double... parameters);
}
