/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import java.util.Random;

public enum Distributions {
  NORMAL {
    @Override
    protected Distribution create(double... parameters) {
      final Random random = new Random(seed);
      return () -> parameters[1] * random.nextGaussian() + parameters[0];
    }
  },
  POISSON {
    @Override
    protected Distribution create(double... parameters) {
      final Random random = new Random(seed);
      return () -> -(Math.log(random.nextDouble()) / parameters[0]);
    }
  };

  private static final long seed = 5388928120325255124L;

  public Distribution of(double... parameters) {
    return create(parameters);
  }

  protected abstract Distribution create(double... parameters);
}
