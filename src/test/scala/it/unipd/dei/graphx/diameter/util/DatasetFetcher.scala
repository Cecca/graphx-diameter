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

import it.unipd.dei.graphx.diameter.Distance
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{Logging, SparkContext}

import scala.language.postfixOps
import scala.sys.process._

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
    GraphLoader.edgeListFile(sc, file.getAbsolutePath, canonicalOrientation = true)
      .mapEdges{ e => 1.0 }
  }

  def get(sc: SparkContext, source: String): Graph[Int, Distance] =
    buildGraph(sc, download(source))

}

abstract class Dataset(val source: String, val diameter: Double) {

  def get(sc: SparkContext): Graph[Int, Distance] = DatasetFetcher.get(sc, source)

}

case class Egonet() extends Dataset(
  source   = "http://snap.stanford.edu/data/facebook_combined.txt.gz",
  diameter = 8)

case class Dblp() extends Dataset(
  source   = "http://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz",
  diameter = 21)

case class Amazon() extends Dataset(
  source  = "http://snap.stanford.edu/data/bigdata/communities/com-amazon.ungraph.txt.gz",
  diameter = 44)