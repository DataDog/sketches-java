package com.datadoghq.sketch.ddsketch.footprint;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.PaginatedStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.openjdk.jol.info.GraphLayout;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleFunction;
import java.util.stream.Stream;

import static com.datadoghq.sketch.ddsketch.footprint.Distributions.*;
import static java.util.concurrent.TimeUnit.*;


public class FootprintTest {

    public static Stream<Arguments> parameters() {
        // cross product of distribution, sketch constructor and timeunit
        return Stream.of(
                POINT.of(42),
                NORMAL.of(0, 1),
                NORMAL.of(100, 2),
                NORMAL.of(100, 50),
                NORMAL.of(100, 50)
                        .composeWith(NORMAL.of(1000, 100)),
                NORMAL.of(100, 50)
                        .composeWith(NORMAL.of(1000, 100))
                        .composeWith(NORMAL.of(10000, 1000)),
                UNIFORM.of(100),
                POISSON.of(0.1),
                POISSON.of(0.5),
                POISSON.of(0.9),
                POISSON.of(0.1)
                        .composeWith(POISSON.of(0.9)),
                POISSON.of(0.5)
                        .composeWith(POISSON.of(0.9)),
                POISSON.of(0.7)
                        .composeWith(POISSON.of(0.9)),
                POISSON.of(0.01)
                        .composeWith(POISSON.of(0.99)),
                POISSON.of(0.001)
                        .composeWith(POISSON.of(0.999))
        ).flatMap(dist -> Stream.of(DDSketch::balanced, DDSketch::memoryOptimal,
                re -> new DDSketch(new BitwiseLinearlyInterpolatedMapping(re), PaginatedStore::new),
                (DoubleFunction<DDSketch>) DDSketch::fast)
                .flatMap(ctor -> Stream.of(NANOSECONDS, MICROSECONDS, MILLISECONDS)
                        .map(timeUnit -> Arguments.of(timeUnit, dist, ctor))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    public void testFootprint(TimeUnit timeUnit, Distribution distribution, DoubleFunction<DDSketch> sketchConstructor) {
        for (double relativeError = 1e-5; relativeError < 1e-2; relativeError *= 10) {
            DDSketch sketch = sketchConstructor.apply(relativeError);
            for (int i = 0; i < 100_000; ++i) {
                long nanos = timeUnit.toNanos(Math.round(Math.abs(distribution.nextValue())));
                sketch.accept(nanos);
            }
            printFootprint(distribution, sketch);
        }
    }

    private static void printFootprint(Distribution distribution, Object instance) {
        GraphLayout layout = GraphLayout.parseInstance(instance);
        System.out.println(distribution);
        System.out.println(layout.toFootprint());
    }
}
