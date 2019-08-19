# sketches-java

This repo contains Java implementations of the distributed quantile sketch algorithms `DDSketch` [1] and `GKArray` [2]. Both sketches are mergeable, meaning that multiple sketches from distributed systems can be combined in a central node.

## DDSketch

DDSketch has relative error guarantees: it computes quantiles with a controlled relative error.

For instance, using a `DDSketch` with a relative accuracy guarantee set to 1%, if the expected quantile value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value is 1000, the computed quantile value is guaranteed to be between 990 and 1010.

A `DDSketch` works by mapping floating-point input values to bins and counting the number of values for each bin. The mapping to bins is handled by `IndexMapping`, while the underlying structure that keeps track of bin counts is `Store`. The standard parameters of the sketch, provided by `DDSketch.standard`, should work in most cases. For using a specific `IndexMapping` or a specific implementation of `Store`, the constructor can be used.

The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the more bins are needed to keep track of the input values. As a rough estimate, if working on durations using standrad parameters (mapping and store) with a relative accuracy of 2%, about 2.2kB (297 bins) are needed to cover values between 1 millisecond and 1 minute, and about 6.8kB (867 bins) to cover values between 1 nanosecond and 1 day. The number of bins that are maintained can be upper-bounded using collapsing stores (see for example `DDSketch.standardCollapsingLowest` and `DDSketch.standardCollapsingHighest`).

## GKArray

GKArray is a sketch with rank error guarantees. Refer to [2] for more details.

## References
[1] Charles Masson, Jee Rim and Homin K. Lee. All the nines: a fully mergeable quantile sketch with relative-error guarantees for arbitrarily large quantiles. 2018.

[2] Michael B. Greenwald and Sanjeev Khanna. Space-efficient online computation of quantile summaries. In Proc. 2001 ACM
SIGMOD International Conference on Management of Data, SIGMOD ’01, pages 58–66. ACM, 2001.
