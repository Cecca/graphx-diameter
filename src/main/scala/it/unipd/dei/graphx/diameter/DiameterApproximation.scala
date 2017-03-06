/*
 * Copyright 2015 Matteo Ceccarello
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package it.unipd.dei.graphx.diameter

import org.apache.spark.graphx.Graph
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Functions to approximate the diameter of large graphs. The algorithm implemented here
 * is based on the ones described in the papers
 *
 *  - "Space and Time Efficient Parallel Graph Decomposition, Clustering,
 *    and Diameter Approximation"<br />
 *    by Matteo Ceccarello, Andrea Pietracaprina, Geppino Pucci, and Eli Upfal
 *  - "A Practical Parallel Algorithm for Diameter Approximation of Massive Weighted Graphs"<br />
 *    by Matteo Ceccarello, Andrea Pietracaprina, Geppino Pucci, and Eli Upfal
 *
 * The functions provided by this object work on weighted graphs where edge weights are of type
 * [[Distance]] (i.e. `Double`). The data associated with vertices is ignored.
 *
 * @author Matteo Ceccarello
 */
object DiameterApproximation {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Computes the average edge weight in a given graph, to be used as a default value for the
   * `delta` parameter of [[run()]]
   * @param graph a weighted graph
   * @tparam VD data associated to vertices is ignored
   * @return the average edge weight
   */
  private def averageWeight[VD:ClassTag](graph: Graph[VD, Distance]): Distance = {
    log.info("Computing average edge weight")
    val start = System.currentTimeMillis()
    val weightSum: Distance = graph.edges.map { edge =>
      edge.attr
    }.reduce(_ + _)
    val avgWeight = weightSum / graph.numEdges
    val end = System.currentTimeMillis()
    val elapsed = end - start
    log.info(s"Average edge weight is $avgWeight (computed in $elapsed ms)")
    avgWeight
  }

  /**
   * Runs the approximation algorithm with default values.
   *
   * The default values are `4000` for `target` and the average edge
   * weight for `delta`.
   *
   * @param graph a weighted graph
   * @tparam VD data associated to vertices is ignored
   * @return an approximation to the diameter of the graph
   */
  def run[VD:ClassTag](graph: Graph[VD, Distance]): Distance =
    run(graph, 4000, averageWeight(graph))

  /**
   * Runs the clustering algorithm with the specified target quotient size.
   *
   * In this case `delta` defaults to the average edge weight.
   *
   * @param graph a weighted graph
   * @param target the target size for the quotient graph
   * @tparam VD data associated to vertices is ignored
   * @return an approximation to the diameter of the graph
   */
  def run[VD:ClassTag](graph: Graph[VD, Distance], target: Long): Distance =
    run(graph, target, averageWeight(graph))

  /**
   * Runs the algorithm with the specified delta parameter.
   *
   * The algorithm runs with the default value for `target`, 4000.
   *
   * @param graph a weighted graph
   * @param delta the parameter for the underlying delta-stepping like algorithm
   * @tparam VD data associated to vertices is ignored
   * @return an approximation to the diameter of the graph
   */
  def run[VD:ClassTag](graph: Graph[VD, Distance], delta: Distance): Distance =
    run(graph, 4000, delta)

  /**
   * Runs the diameter approximation algorithm. The two parameters have the following
   * meaning:
   *
   * - `target`: this is the size of the quotient graph that will be built
   *   by the underlying clustering algorithm. It depends on the size of
   *   the local memory of the machines. The last step of the algorithm
   *   computes the diameter of a graph of size `target`. Higher values of
   *   `target` can result is shorter running times, whereas smaller ones
   *   require less memory.
   *
   * - `delta`: this parameter, representing a distance,
   *   controls the number of nodes and edges that can be active in each
   *   step of the algorithm. Intuitively, higher values will result in
   *   fewer but slower rounds; smaller values will perform more shorter
   *   rounds. In any case, this parameter is taken as a hint by the
   *   algorithm, that then auto-tunes itself.
   *
   * Further details are provided in the companion papers.
   *
   * @param graph a weighted graph
   * @param target the target size for the quotient graph
   * @param delta the parameter for the underlying delta-stepping like algorithm
   * @tparam VD data associated to vertices is ignored
   * @return an approximation to the diameter of the graph
   */
  def run[VD:ClassTag](graph: Graph[VD, Distance], target: Long, delta: Distance): Distance = {
    val clustered = Clustering.run(graph, target, delta)

    log.info("Start compute diameter")
    val localGraph = LocalGraph.fromGraph(clustered)
    val n = localGraph.size

    val dists = Dijkstra.distributedApsp(graph.vertices.sparkContext, localGraph)
    val rads = localGraph.radiuses

    var diameter = 0.0

    var i = 0
    var j = 0
    while (i<n) {
      while (j<n) {
        diameter = math.max(diameter, dists(i)(j) + rads(i) + rads(j))
        j += 1
      }
      i += 1
    }

    diameter
  }

  /**
   * Provides an implicit conversion to a [[DiameterApproximator]] object
   * that allows to call the `diameterApprox`
   * function on a [[Graph]] instance
   *
   * @param graph the graph to wrap in a [[DiameterApproximator]]
   * @tparam VD any vertex data is ignored
   * @return a [[DiameterApproximator]] wrapping the given graph
   */
  implicit def graphToApproximator[VD:ClassTag](graph: Graph[VD, Distance])
  : DiameterApproximator[VD] = new DiameterApproximator[VD](graph)

}

/**
 * Provides facilities to call diameter approximation functions directly from
 * a [[Graph]] object.
 *
 * @param graph the graph on which the diameter will be computed
 * @tparam VD any vertex data is ignored
 */
class DiameterApproximator[VD:ClassTag](graph: Graph[VD, Distance]) {

  /**
   * Equivalent to [[DiameterApproximation.run()]] with no arguments
   * @return The approximation to the graph diameter
   */
  def diameterApprox(): Distance =
    DiameterApproximation.run(graph)

  /**
   * Equivalent to [[DiameterApproximation.run()]] with the target argument
   * @return The approximation to the graph diameter
   */
  def diameterApprox(target: Long): Distance =
    DiameterApproximation.run(graph, target)

  /**
   * Equivalent to [[DiameterApproximation.run()]] with the delta argument
   * @return The approximation to the graph diameter
   */
  def diameterApprox(delta: Distance): Distance =
    DiameterApproximation.run(graph, delta)

  /**
   * Equivalent to [[DiameterApproximation.run()]] with both the target
   * and delta arguments
   * @return The approximation to the graph diameter
   */
  def diameterApprox(target: Long, delta: Distance): Distance =
    DiameterApproximation.run(graph, target, delta)

}
