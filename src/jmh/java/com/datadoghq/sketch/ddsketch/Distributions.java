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
