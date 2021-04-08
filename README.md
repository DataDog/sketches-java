# sketches-java

This repo contains Java implementations of the distributed quantile sketch algorithm `DDSketch` [1]. DDSketch is mergeable, meaning that multiple sketches from distributed systems can be combined in a central node.

# Quick start guide

`sketches-java` is available in the Maven Central Repository. See [this page](https://search.maven.org/artifact/com.datadoghq/sketches-java) for easily adding it as a dependency to your project. You should then be able to import `com.datadoghq.sketch.ddsketch.DDSketch`.

The following code snippet shows the most basic features of `DDSketch`:

```java
// Creating an initially empty sketch, with low memory footprint
double relativeAccuracy = 0.01;
DDSketch sketch = DDSketches.unboundedDense(relativeAccuracy);

// Adding values to the sketch
sketch.accept(3.2); // adds a single value
sketch.accept(2.1, 3); // adds multiple times the same value

// Querying the sketch
sketch.getValueAtQuantile(0.5); // returns the median value
sketch.getMinValue();
sketch.getMaxValue();

// Merging another sketch into the sketch, in-place
DDSketch anotherSketch = DDSketch.unboundedDense(relativeAccuracy);
DoubleStream.of(3.4, 7.6, 2.8).forEach(anotherSketch);
sketch.mergeWith(anotherSketch);
```

# DDSketch

DDSketch has relative error guarantees: it computes quantiles with a controlled relative error.

For instance, using `DDSketch` with a relative accuracy guarantee set to 1%, if the expected quantile value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value is 1000, the computed quantile value is guaranteed to be between 990 and 1010.

`DDSketch` works by mapping floating-point input values to bins and counting the number of values for each bin. The mapping to bins is handled by `IndexMapping`, while the underlying structure that keeps track of bin counts is `Store`. `DDSketches.unboundedDense()` constructs a sketch whose bin counts are backed by an array, therefore offering constant-time insertion. The array is grown as necessary to accommodate for the range of input values.  

The size of the sketch can be upper-bounded by using collapsing stores. For instance, `DDSketches.logarithmicCollapsingLowestDense()` is the version of `DDSketch` described in the [DDSketch paper](http://www.vldb.org/pvldb/vol12/p2195-masson.pdf). It collapses lowest bins when the maximum number of buckets is reached. See the [`DDSketches`](src/main/java/com/datadoghq/sketch/ddsketch/DDSketches.java) for more preset sketches and more details.

The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the more bins are needed to keep track of the input values. As a rough estimate, if working on durations using `DDSketches.unboundedDense(0.02)` (relative accuracy of 2%), about 2kB (275 bins) are needed to cover values between 1 millisecond and 1 minute, and about 6kB (802 bins) to cover values between 1 nanosecond and 1 day. The number of bins that are maintained can be upper-bounded using collapsing stores (see for example `DDSketches.collapsingLowestDense()` and `DDSketches.collapsingHighestDense()`).

# References
[1] Charles Masson, Jee E. Rim and Homin K. Lee. DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees. 2019.
