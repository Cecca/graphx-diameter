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

import org.apache.spark.Logging
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object DiameterApproximation extends Logging {

  private def averageWeight[VD:ClassTag](graph: Graph[VD, Distance]): Distance = {
    logInfo("Computing average edge weight")
    val start = System.currentTimeMillis()
    val weightSum: Distance = graph.edges.map { edge =>
      edge.attr
    }.reduce(_ + _)
    val avgWeight = weightSum / graph.numEdges
    val end = System.currentTimeMillis()
    val elapsed = end - start
    logInfo(s"Average edge weight is $avgWeight (computed in $elapsed ms)")
    avgWeight
  }

  def run[VD:ClassTag](graph: Graph[VD, Distance]): Distance =
    run(graph, 4000, averageWeight(graph))

  def run[VD:ClassTag](graph: Graph[VD, Distance], target: Long): Distance =
    run(graph, target, averageWeight(graph))

  def run[VD:ClassTag](graph: Graph[VD, Distance], delta: Distance): Distance =
    run(graph, 4000, delta)

  def run[VD:ClassTag](graph: Graph[VD, Distance], target: Long, delta: Distance): Distance = {
    val clustered = Clustering.run(graph, target, delta)

    logInfo("Start compute diameter")
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

}
