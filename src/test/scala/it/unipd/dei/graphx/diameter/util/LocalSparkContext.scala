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

import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.{Logging, SparkConf, SparkContext}

object LocalSparkContext extends Logging {

  def withSpark[T](f: SparkContext => T): T = {
    val cores = Runtime.getRuntime().availableProcessors()
    val par = 2 * cores
    val conf = new SparkConf().set("spark.default.parallelism", par.toString)
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext("local", "test", conf)
    logInfo(s"Spark parallelism ${sc.defaultParallelism}")
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }

}
