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

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable.ArrayBuffer

/**
 * An implementation of Dijkstra's Single Source Shortest Path algorithm.
 */
private[diameter]
object Dijkstra {

  def distributedApsp(sc: SparkContext, graph: LocalGraph): Array[Array[Distance]] = {
    val bGraph = sc.broadcast(graph)
    val ids = sc.parallelize(Range(0, graph.size))

    ids.map { id =>
      (id, sssp(id, bGraph.value))
    }.collect().sortBy(_._1).map(_._2)
  }

  def sssp(src: Int, graph: LocalGraph): Array[Distance] = {

    val q = new PriorityQueue(graph.size)
    val distance = Array.fill[Distance](graph.size)(Infinity)
    distance(src) = 0

    for(v <- Range(0, graph.size)) {
      q.enqueue(v, Infinity)
    }

    q.decreasePriority(src, 0)

    while(q.nonEmpty) {
      val vertex = q.dequeue()
      val nWeights = graph.weights(vertex)
      val nIds = graph.adjacency(vertex)
      var i = 0
      while(i < nWeights.length) {
        val w = nWeights(i)
        val v = nIds(i)
        val d = w + distance(vertex)
        if(d < distance(v)) {
          distance(v) = d
          q.decreasePriority(v, d)
        }
        i += 1
      }
    }

    distance
  }

}

/**
 * Store information on the graph as two two-dimensional arrays, one for the
 * weights and one for the adjacency. Note that these two-dimensional arrays
 * are not matrices, in the sense that the inner arrays have different lengths.
 *
 * This class is used in place of a single double array of tuples to limit
 * memory allocation and object creation. This way the overhead of object
 * creation for tuples in the inner loop of Dijkstra's algorithm is avoided
 * and so is the garbage collection of these objects. This leads to better
 * performance.
 */
private[diameter]
class LocalGraph(
                  val adjacency: Array[Array[Int]],
                  val weights: Array[Array[Distance]],
                  val radiuses: Array[Distance])
  extends Serializable {

  def size: Int = weights.length

}

private[diameter]
class LocalGraphBuilder(
                         val adjacency: Array[ArrayBuffer[Int]],
                         val weights: Array[ArrayBuffer[Distance]],
                         val radiuses: Array[Distance]) {

  def addEdge(u: Int, v: Int, w: Distance): Unit = {
    // update the neighbourhood of u
    adjacency(u).append(v)
    weights(u).append(w)
    // update, symmetrically, the neighbourhood of v
    adjacency(v).append(u)
    weights(v).append(w)
  }

  def setRadius(u: Int, r: Distance): Unit = {
    radiuses(u) = r
  }

  def freeze(): LocalGraph =
    new LocalGraph(
      adjacency.map(_.toArray),
      weights.map(_.toArray),
      radiuses
    )
}

private[diameter]
object LocalGraphBuilder {

  def apply(n: Int): LocalGraphBuilder =
    new LocalGraphBuilder(
      Array.fill(n)(ArrayBuffer[Int]()),
      Array.fill(n)(ArrayBuffer[Distance]()),
      Array.fill[Distance](n)(0.0)
    )

}

private[diameter]
object LocalGraph {

  def fromGraph(graph: Graph[ClusteringInfo, Distance]): LocalGraph = {

    val reweighted = graph.triplets.flatMap { triplet =>
      val w = triplet.srcAttr.distance + triplet.dstAttr.distance + triplet.attr
      val c1 = triplet.srcAttr.center
      val c2 = triplet.dstAttr.center
      if (c1 < c2) {
        Iterator(((c1, c2), w))
      } else if (c1 > c2) {
        Iterator(((c2, c1), w))
      } else {
        Iterator.empty
      }
    }.reduceByKey(math.min).collect()

    val radiusInfo = graph.vertices.map({case (_, v) => (v.center, v.distance)})
      .reduceByKey(math.max).collect()

    val idRemapping: Map[VertexId, Int] = {
      val m = scala.collection.mutable.Map[VertexId, Int]()
      var cnt = 0
      for ((v, d) <- radiusInfo) {
        require(v >= 0, s"Negative vertex id $v")
        require(d >= 0, s"Negative radius $d")
        if(!m.contains(v)) {
          m.update(v, cnt)
          cnt += 1
        }
      }
      m.toMap
    }

    val builder = LocalGraphBuilder(idRemapping.size)

    for (((u, v), w) <- reweighted) {
      require(w > 0, "Negative weights are not allowed!")
      builder.addEdge(idRemapping(u), idRemapping(v), w)
    }

    for((id, radius) <- radiusInfo) {
      builder.setRadius(idRemapping(id), radius)
    }

    builder.freeze()
  }

}
