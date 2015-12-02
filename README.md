Graph diameter approximation on Spark
=====================================

[![Build Status](https://travis-ci.org/Cecca/graphx-diameter.svg)](https://travis-ci.org/Cecca/graphx-diameter)

`graphx-diameter` is a Spark package that allows to approximate the
diameter of (weighted) graphs, that is the longest shortest path.

The algorithm implemented here is derived from the ones described in
the following papers

 - _Space and Time Efficient Parallel Graph Decomposition, Clustering,
   and Diameter Approximation_ <br />
   Matteo Ceccarello, Andrea Pietracaprina, Geppino Pucci, and Eli
   Upfal <br />
   [SPAA 2015](http://dx.doi.org/10.1145/2755573.2755591)

 - _A Practical Parallel Algorithm for Diameter Approximation of
   Massive Weighted Graphs_ <br />
   Matteo Ceccarello, Andrea Pietracaprina, Geppino Pucci, and Eli
   Upfal <br />
   [Arxiv preprint](http://arxiv.org/abs/1506.03265)

**NOTE**: the implementation contained in this package is *not* the
  one used to perform the experiments described in the aforementioned
  papers. That implementation is available under the GPL license
  [here](http://crono.dei.unipd.it/gradias/), and was developed on
  plain Spark. `graphx-diameter`, instead, provides an equivalent
  implementation compatible with `graphx`.
  

Motivation
----------

The diameter of a graph can be obtained by computing all pairs
shortest paths. However, computing APSP is impractical on large
graphs, due to the excessive space and time requirements.

To compute an approximation to the diameter using only linear space,
one can resort to a simple Single Source Shortest Path (SSSP)
computation, that approximates the diameter within a factor of
two. The drawback of this approach is that it requires a number of
rounds linear in the diameter itself: on a platform such as Spark,
where for efficiency we seek to minimize the number of rounds, this
is undesirable.

A popular approach to approximate the diameter (and some centrality
measures) are algorithms based on probabilistic counters, like
[HyperANF](http://dl.acm.org/citation.cfm?doid=1963405.1963493)
[Boldi, Rosa, Vigna - WWW11]. These
algorithms are able to attain a tight estimate of the
diameter. However, in a distributed computing setting like Spark, the
running time linear in the diameter and the superlinear space
requirements limit the applicability of this approach.

Therefore, we developed the algorithm implemented in this library with
two goals:

 - performing a number of rounds sublinear in the diameter
 - using space linear in the size of the graph

The algorithm returns an approximation of the diameter in the form of
an upper bound, with a provable polylogarithmic bound. In practice,
the approximation factor is `~2` on unweighted graphs and `< 1.5` on
weighted graphs.

Further details on the algorithm, its efficiency, and the
approximation factor are given in the aforementioned papers:

 - For the unweighted case: [_"Space and Time Efficient Parallel Graph
   Decomposition, Clustering, and Diameter
   Approximation"_](http://dx.doi.org/10.1145/2755573.2755591);

 - For the weighted case: [_"A Practical Parallel Algorithm for
   Diameter Approximation of Massive Weighted
   Graphs"_](http://arxiv.org/abs/1506.03265).


Linking
-------

**TODO**


Usage
-----

The library works on graphs with `Double` edge weights assigned to
edges. The package `it.unipd.dei.graphx.diameter` defines a type
`Distance` like the following

```scala
type Distance = Double
```

The algorithm takes two parameters, namely

 - `target`: this is the size of the quotient graph that will be built
   by the underlying clustering algorithm. It depends on the size of
   the local memory of the machines. The last step of the algorithm
   computes the diameter of a graph of size `target`. Higher values of
   `target` can result is shorter running times, whereas smaller ones
   require less memory. Usually `target == 4000` provides a good
   compromise, and this is the default.

 - `delta`: this is a distance and controls the number of edges
   controls the number of nodes and edges that can be active in each
   step of the algorithm. Intuitively, higher values will result in
   fewer but slower rounds; smaller values will perform more shorter
   rounds. In any case, this parameter is taken as a hint by the
   algorithm, that then auto-tunes itself. A good initial guess is
   (empirically) the average edge weight, which is the default.

For more details on these two parameters, we refer to the companion
papers.

Given a `org.apache.spark.graphx.Graph[V, Distance]` object, you can
get an approximation to its diameter as follows, using implicit
conversions

```scala
// import implicit conversions
import it.unipd.dei.graphx.diameter.DiameterApproximation._

val g = // ... build the graph object ...

// Compute the approximation using the default parameters
g.diameterApprox()

// Specify the target size for the underlying clustering algorithm
g.diameterApprox(target=5000)

// Control the number of active nodes/edges in each step
g.diameterApprox(delta=0.5)

// Both parameters can be specified simultaneously
g.diameterApprox(target=5000, delta=0.5)
```

If you prefer to avoid implicit conversions, you can explicitly invoke
`DiameterApproximation.run`, as follows

```scala
import it.unipd.dei.graphx.diameter.DiameterApproximation

val g = // ... build the graph object ...

// Compute the approximation using the default parameters
DiameterApproximation.run(g)

// Specify the target size for the underlying clustering algorithm
DiameterApproximation.run(g, target=5000)

// Control the number of active nodes/edges in each step
DiameterApproximation.run(g, delta=0.5)

// Both parameters can be specified simultaneously
DiameterApproximation.run(g, target=5000, delta=0.5)
```
