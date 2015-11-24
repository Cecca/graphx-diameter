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

import it.unipd.dei.graphx.diameter.util.LocalSparkContext.withSpark
import it.unipd.dei.graphx.diameter.util.{Amazon, Dblp, Egonet}
import org.scalatest.{FreeSpec, Matchers}

class DiameterApproximationSpec extends FreeSpec with Matchers {

  "The diameter approximation on:" - {
    "egonet" - {
      withSpark { sc =>
        val dataset = Egonet()
        val g = dataset.get(sc)
        val approx = DiameterApproximation.run(g, 100)
        val original = dataset.diameter

        "should be greater than the original" in {
          approx should be >= original
        }

        "should be smaller than twice the original" in {
          approx should be <= (2*original)
        }
      }
    }
    "dblp" - {
      withSpark { sc =>
        val dataset = Dblp()
        val g = dataset.get(sc)
        val approx = DiameterApproximation.run(g, 100)
        val original = dataset.diameter

        "should be greater than the original" in {
          approx should be >= original
        }

        "should be smaller than twice the original" in {
          approx should be <= (2*original)
        }
      }
    }
    "amazon" - {
      withSpark { sc =>
        val dataset = Amazon()
        val g = dataset.get(sc)
        val approx = DiameterApproximation.run(g, 100)
        val original = dataset.diameter

        "should be greater than the original" in {
          approx should be >= original
        }

        "should be smaller than twice the original" in {
          approx should be <= (2*original)
        }
      }
    }
  }

}
