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
import it.unipd.dei.graphx.diameter.util._
import org.scalatest.{FreeSpec, Matchers}

class DiameterApproximationSpec extends FreeSpec with Matchers {

  def test(dataset: Dataset) = {
    withSpark { sc =>
      val g = dataset.get(sc)
      val approx = DiameterApproximation.run(g, 100)
      val original = dataset.diameter(sc)
      val twice = 2*original

      f"should be greater than the original ($approx%.2f >= $original%.2f ?)" in {
        approx should be >= original
      }

      f"should be smaller than twice the original ($approx%.2f <= $twice%.2f ?) " in {
        approx should be <= twice
      }
    }

  }

  "The diameter approximation on unweighted graphs:" - {
    "egonet" - {
      test(new Egonet())
    }
    "dblp" - {
      test(new Dblp())
    }
    "amazon" - {
      test(new Amazon())
    }
  }

  "The diameter approximation on graphs with uniform random weights:" - {
    "egonet" - {
      test(new EgonetUniform())
    }
    "dblp" - {
      test(new DblpUniform())
    }
    "amazon" - {
      test(new AmazonUniform())
    }
  }

}
