package com.datadoghq.sketch.util.dataset;

import java.util.Objects;
import java.util.function.DoubleSupplier;

public class SyntheticDataset implements Dataset {

    private final DoubleSupplier doubleSupplier;

    public SyntheticDataset(DoubleSupplier doubleSupplier) {
        this.doubleSupplier = Objects.requireNonNull(doubleSupplier);
    }

    public double get() {
        return doubleSupplier.getAsDouble();
    }

    @Override
    public double[] get(int size) {
        final double[] values = new double[size];
        for (int i = 0; i < size; i++) {
            values[i] = get();
        }
        return values;
    }
}
