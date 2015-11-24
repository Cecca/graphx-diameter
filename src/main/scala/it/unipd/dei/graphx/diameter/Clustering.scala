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

import org.apache.spark.graphx.{Graph, TripletFields, VertexId}
import org.apache.spark.{Accumulator, Logging}

import scala.annotation.tailrec
import scala.util.Random

private[diameter]
object Clustering extends Logging {

  def run[VD](graph: Graph[VD, Distance], target: Long, delta: Distance = 1.0)
  : Graph[ClusteringInfo, Distance] = {

    val maxIterations = math.ceil(math.log(graph.ops.numVertices / target) / math.log(2))
    val batchDim = target / maxIterations

    logInfo(s"Start clustering (iterations=$maxIterations, batch=$batchDim)")
    val clustered = cluster(init(graph), target, delta, batchDim)

    clustered
  }

  private def init[VD](graph: Graph[VD, Distance]): Graph[ClusteringInfo, Distance] =
    graph.mapVertices({ case _ => ClusteringInfo() }).groupEdges(math.min)

  @tailrec
  private def cluster(
                       graph: Graph[ClusteringInfo, Distance],
                       target: Long,
                       delta: Distance,
                       batchDim: Double)
  : Graph[ClusteringInfo, Distance] = {

    logInfo("Start clustering a fraction of the graph")

    val quotientSize = graph.vertices.filter({ case (_, v) => v.isQuotient }).count()
    val numUncovered = graph.vertices.filter({ case (_, v) => !v.covered }).count()

    if (quotientSize < target) {
      return selectCenters(graph, 1.0)
    }

    // Add the next batch of centers
    val centerProb = batchDim / numUncovered
    val wCenters = selectCenters(graph, centerProb)

    val (updated, newDelta) = phase(wCenters, math.max(numUncovered / 2, target), delta)

    // Reset the nodes
    val reset = resetGraph(updated)

    cluster(reset, target, newDelta, batchDim)
  }

  private def resetGraph(updated: Graph[ClusteringInfo, Distance])
  : Graph[ClusteringInfo, Distance] = {
    updated.mapVertices { case (id, v) =>
      if (!v.covered) {
        ClusteringInfo()
      } else {
        v.copy(offsetDistance = v.distance, phaseDistance = 0, updated = true)
      }
    }
  }

  private def selectCenters(graph: Graph[ClusteringInfo, Distance], centerProb: Double) = {
    graph.mapVertices { case (id, v) =>
      if (!v.covered && Random.nextDouble() <= centerProb) {
        ClusteringInfo.makeCenter(id, v)
      } else {
        v
      }
    }
  }

  @tailrec
  private def phase(
                     graph: Graph[ClusteringInfo, Distance],
                     phaseTarget: Long,
                     delta: Distance)
  : (Graph[ClusteringInfo, Distance], Distance) = {

    tentative(graph, phaseTarget, delta) match {
      case Right(g) => (g, delta)
      case Left(g) =>
        val activated = g.mapVertices { case (_, v) =>
          if (v.covered) {
            v.copy(updated = true)
          } else {
            v
          }
        }
        phase(activated, phaseTarget, 2 * delta)
    }
  }

  private def tentative(
                         graph: Graph[ClusteringInfo, Distance],
                         phaseTarget: Long,
                         delta: Distance)
  : Either[Graph[ClusteringInfo, Distance], Graph[ClusteringInfo, Distance]] = {

    logInfo(s"Doing a tentative with delta=$delta and target $phaseTarget")

    val updatedGraph = deltaStep(graph, delta, phaseTarget)

    val quotientSize = updatedGraph.vertices.filter({ case (_, v) => v.isQuotient }).count()

    if (quotientSize <= phaseTarget) {
      Right(updatedGraph)
    } else {
      Left(updatedGraph)
    }
  }

  /*
   * A delta step can exit in one of two states:
   *
   *  - We are below the target size
   *  - The last relaxation did not update any vertex
   *
   * In the first case we can go on with the clustering, whereas the second scenario can
   * arise in two different contexts:
   *
   *  - each cluster has reached all the nodes it could, i.e. uncovered nodes
   *    are in a different connected component
   *  - the selected delta do not allow to cover half the graph
   *
   * To detect the first case, one should look at the number of edges that can lead from
   * nodes inside a cluster to uncovered nodes.
   *
   */
  @tailrec
  private def deltaStep(
                         graph: Graph[ClusteringInfo, Distance],
                         delta: Distance,
                         phaseTarget: Long)
  : Graph[ClusteringInfo, Distance] = {

    val start = System.currentTimeMillis()

    val updatedAcc = graph.edges.sparkContext.accumulator(0L)
    val quotientAcc = graph.edges.sparkContext.accumulator(0L)

    val updated = relaxEdges(graph, delta, updatedAcc, quotientAcc).persist()
    updated.numVertices

    val updatedCnt = updatedAcc.value
    val quotientSize = quotientAcc.value

    val end = System.currentTimeMillis()

    logInfo(s"Delta step: ${end - start}ms elapsed ")

    if (updatedCnt == 0 || quotientSize <= phaseTarget) {
      updated
    } else {
      deltaStep(updated, delta, phaseTarget)
    }
  }

  private def relaxEdges(
                          graph: Graph[ClusteringInfo, Distance],
                          delta: Distance,
                          updatedAcc: Accumulator[Long],
                          quotientAcc: Accumulator[Long])
  : Graph[ClusteringInfo, Distance] = {

    val messages = graph
      .subgraph(epred = { ctx => ctx.attr <= delta })
      .aggregateMessages[ClusteringMessage](
        ctx => {
          if (ctx.srcAttr.updated && ctx.srcAttr.phaseDistance <= delta) {
            ctx.sendToDst(ClusteringMessage(ctx.srcAttr, ctx.attr))
          }
          if (ctx.dstAttr.updated && ctx.dstAttr.phaseDistance <= delta) {
            ctx.sendToSrc(ClusteringMessage(ctx.dstAttr, ctx.attr))
          }
        },
        ClusteringMessage.min,
        TripletFields.All
      )

    graph.outerJoinVertices(messages) {
      case (id, v, Some(msg)) if !v.covered || (msg.distance < v.distance && v.isUnstable) =>
        updatedAcc += 1
        if (v.isQuotient) quotientAcc += 1
        v.updateWith(msg, delta)
      case (_, v, _) if v.phaseDistance <= delta =>
        if (v.isQuotient) quotientAcc += 1

        v.copy(updated = false)
      case (_, v, _) =>
        if (v.isQuotient) quotientAcc += 1
        v
    }
  }

}

private[diameter]
case class ClusteringMessage(
                              center: VertexId,
                              phaseDistance: Distance,
                              offsetDistance: Distance) {

  def distance: Distance = phaseDistance + offsetDistance

}

private[diameter]
object ClusteringMessage {

  def apply(v: ClusteringInfo, edgeWeight: Distance): ClusteringMessage =
    new ClusteringMessage(v.center, edgeWeight + v.phaseDistance, v.offsetDistance)

  def min(a: ClusteringMessage, b: ClusteringMessage): ClusteringMessage =
    if (a.distance < b.distance) a
    else b

}

/**
 * Contains various information about the role of the vertex in the clustering.
 */
private[diameter]
case class ClusteringInfo(
                           center: VertexId,
                           phaseDistance: Distance,
                           offsetDistance: Distance,
                           updated: Boolean,
                           covered: Boolean) {

  /**
   * The distance from the center. To get the center ID, use `center`.
   * @return the distance from the cluster's center.
   */
  def distance: Distance = phaseDistance + offsetDistance

  /**
   * `true` if the node is itself a center
   * @return true if the node is itself a center.
   */
  def isCenter: Boolean = distance == 0.0

  def isQuotient: Boolean = !covered || isCenter

  def isUnstable: Boolean = offsetDistance == 0.0 && !isCenter

  def updateWith(message: ClusteringMessage, delta: Distance): ClusteringInfo =
    this.copy(
      center = message.center,
      phaseDistance = message.phaseDistance,
      offsetDistance = message.offsetDistance,
      updated = true,
      covered = covered || message.phaseDistance <= delta)

}

private[diameter]
object ClusteringInfo {

  def apply(): ClusteringInfo = new ClusteringInfo(
    center = -1L,
    phaseDistance = Infinity,
    offsetDistance = 0.0,
    updated = false,
    covered = false
  )

  def makeCenter(id: VertexId, v: ClusteringInfo): ClusteringInfo =
    v.copy(center = id, phaseDistance = 0.0, updated = true, covered = true)

}
