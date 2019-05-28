package com.datadoghq.sketch.util.dataset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public class FileDataset implements Dataset {

    private final double[] values;

    public FileDataset(String path) {
        this(Path.of(path));
    }

    public FileDataset(Path path) {
        try (Stream<String> stream = Files.lines(path)) {
            this.values = stream
                    .mapToDouble(Double::parseDouble)
                    .toArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getMaxSize() {
        return values.length;
    }

    @Override
    public double[] get(int size) {
        final int offset = ThreadLocalRandom.current().nextInt(values.length);
        final double[] subValues = new double[size];
        if (offset + size > values.length) {
            System.arraycopy(values, offset, subValues, 0, values.length - offset);
            System.arraycopy(values, 0, subValues, values.length - offset, size - (values.length - offset));
        } else {
            System.arraycopy(values, offset, subValues, 0, size);
        }
        return subValues;
    }
}
