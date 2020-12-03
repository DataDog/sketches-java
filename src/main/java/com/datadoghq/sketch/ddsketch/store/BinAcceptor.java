package com.datadoghq.sketch.ddsketch.store;

@FunctionalInterface
public interface BinAcceptor {
    void accept(int index, double value);
}
