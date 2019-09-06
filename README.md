# sketches-java

This repo contains Java implementations of the distributed quantile sketch algorithms `DDSketch` [1] and `GKArray` [2]. Both sketches are mergeable, meaning that multiple sketches from distributed systems can be combined in a central node.

# Quick start guide

`sketches-java` is available in the Maven Central Repository. See [this page](https://search.maven.org/artifact/com.datadoghq/sketches-java) for easily adding it as a dependency to your project. You should then be able to import `com.datadoghq.sketch.ddsketch.DDSketch`.

The following code snippet shows the most basic features of `DDSketch`:

```java
// Creating an initially empty sketch, with low memory footprint
double relativeAccuracy = 0.01;
DDSketch sketch = DDSketch.memoryOptimal(relativeAccuracy);

// Adding values to the sketch
sketch.accept(3.2); // adds a single value
sketch.accept(2.1, 3); // adds multiple times the same value

// Querying the sketch
sketch.getValueAtQuantile(0.5); // returns the median value
sketch.getMinValue();
sketch.getMaxValue();

// Merging another sketch into the sketch, in-place
DDSketch anotherSketch = DDSketch.memoryOptimal(relativeAccuracy);
DoubleStream.of(3.4, 7.6, 2.8).forEach(anotherSketch);
sketch.mergeWith(anotherSketch);
```

# DDSketch

DDSketch has relative error guarantees: it computes quantiles with a controlled relative error.

For instance, using `DDSketch` with a relative accuracy guarantee set to 1%, if the expected quantile value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value is 1000, the computed quantile value is guaranteed to be between 990 and 1010.

`DDSketch` works by mapping floating-point input values to bins and counting the number of values for each bin. The mapping to bins is handled by `IndexMapping`, while the underlying structure that keeps track of bin counts is `Store`. `DDSketch.memoryOptimal()` constructs a sketch with a logarithmic index mapping, hence low memory footprint, whereas `DDSketch.fast()` and `DDSketch.balanced()` offer faster ingestion speeds at the cost of larger memory footprints. The size of the sketch can be upper-bounded by using collapsing stores. For instance, {@link #memoryOptimalCollapsingLowest} is the version of `DDSketch` described in the paper, and also implemented in [Go](https://github.com/DataDog/sketches-go/) and [Python](https://github.com/DataDog/sketches-go/). It collapses lowest bins when the maximum number of buckets is reached. For using a specific `IndexMapping` or a specific implementation of `Store`, the constructor can be used.

The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the more bins are needed to keep track of the input values. As a rough estimate, if working on durations using `DDSketch.memoryOptimal(0.02)` (relative accuracy of 2%), about 2kB (275 bins) are needed to cover values between 1 millisecond and 1 minute, and about 6kB (802 bins) to cover values between 1 nanosecond and 1 day. The number of bins that are maintained can be upper-bounded using collapsing stores (see for example `DDSketch.memoryOptimalCollapsingLowest()` and `DDSketch.memoryOptimalCollapsingHighest()`).

# GKArray

GKArray is a sketch with rank error guarantees. Refer to [2] for more details.

# References
[1] Charles Masson, Jee E. Rim and Homin K. Lee. DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees. 2019.

[2] Michael B. Greenwald and Sanjeev Khanna. Space-efficient online computation of quantile summaries. In Proc. 2001 ACM
SIGMOD International Conference on Management of Data, SIGMOD ’01, pages 58–66. ACM, 2001.
