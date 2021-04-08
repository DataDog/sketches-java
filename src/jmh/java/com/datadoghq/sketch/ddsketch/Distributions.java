/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import java.util.concurrent.ThreadLocalRandom;

public enum Distributions {
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

  public Distribution of(double... parameters) {
    return create(parameters);
  }

  protected abstract Distribution create(double... parameters);
}
