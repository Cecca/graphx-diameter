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

package it.unipd.dei.graphx.diameter.util

import java.io.File
import java.net.URL

import it.unipd.dei.graphx.diameter.{Dijkstra, ClusteringInfo, LocalGraph, Distance, Infinity}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.{Logging, SparkContext}

import scala.language.postfixOps
import scala.sys.process._
import scala.collection.mutable
import scala.util.Random

/**
 * Downloads datasets from a remote location.
 */
object DatasetFetcher extends Logging {

  val cacheDir = new File(System.getProperty("user.home"), ".graphx-diameter-datasets")

  if (!cacheDir.isDirectory) {
    cacheDir.mkdirs()
  }

  def download(source: String): File = {
    val input = source
    val output: File = new File(cacheDir, input.split("/").last)

    if (!output.isFile) {
      logInfo(s"Download $input into $output")
      new URL(input) #> output !!
    }
    output
  }

  def buildGraph(sc: SparkContext, file: File): Graph[Int, Distance] = {
    GraphLoader.edgeListFile(
      sc, file.getAbsolutePath, canonicalOrientation = true, numEdgePartitions = 8)
      .mapEdges{ e => 1.0 }
      .groupEdges(math.min)
  }

  def get(sc: SparkContext, source: String): Graph[Int, Distance] =
    buildGraph(sc, download(source))

}

abstract class Dataset(val source: String) extends Logging {

  def get(sc: SparkContext): Graph[Int, Distance] = DatasetFetcher.get(sc, source)

  def localCopy(sc: SparkContext): LocalGraph = {
    val g = get(sc).mapVertices{ (id, v) => ClusteringInfo.makeCenter(id, ClusteringInfo()) }
    LocalGraph.fromGraph(g)
  }

  /*
   * Here we compute a lower bound to the diameter because the source
   * site [SNAP](http://snap.stanford.edu/) provides lower bounds that
   * are actually lower than the ones we can find empirically in this way.
   */
  def diameter(sc: SparkContext): Double = {
    logInfo("Computing a lower bound to the original diameter")
    val graph = localCopy(sc)

    var bestDist: Distance = 0
    var src = Random.nextInt(graph.size)
    var dest = -1
    var connected = true
    var it = 0
    val visited = mutable.Set[Int]()

    while (!visited.contains(src)) {
      it += 1
      visited.add(src)
      val distances = Dijkstra.sssp(src, graph)
      val (far, dist, conn) = farthest(distances)
      // If in a disconnected node, jump to another source
      dest = if (dist > 0) far else Random.nextInt(graph.size)
      connected = connected && conn
      if (dist > bestDist) {
        it = 0
        bestDist = dist
        src = far
      }
    }

    logInfo(s"Found a lower bound on the original diameter: $bestDist")
    bestDist
  }

  private def farthest(distances: Array[Distance]): (Int, Distance, Boolean) = {
    var vVal: Distance = 0
    var vIdx = 0
    var it = 0
    val n = distances.length
    var connected = true
    while(it < n) {
      if(distances(it) < Infinity) {
        if (distances(it) > vVal) {
          vVal = distances(it)
          vIdx = it
        }
      } else {
        connected = false
      }
      it += 1
    }
    (vIdx, vVal, connected)
  }

}

/**
 * Reweights the edges with weights between 1 and 2
 */
trait UniformWeights extends Dataset {
  override def get(sc: SparkContext): Graph[Int, Distance] = {
    super.get(sc).mapEdges { e =>
      Random.nextDouble() + 1
    }
  }
}

class Egonet() extends Dataset(
  "http://snap.stanford.edu/data/facebook_combined.txt.gz")

class Dblp() extends Dataset(
  "http://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz")

class Amazon() extends Dataset(
  "http://snap.stanford.edu/data/bigdata/communities/com-amazon.ungraph.txt.gz")

class EgonetUniform() extends Egonet with UniformWeights
class DblpUniform() extends Dblp with UniformWeights
class AmazonUniform() extends Amazon with UniformWeights
