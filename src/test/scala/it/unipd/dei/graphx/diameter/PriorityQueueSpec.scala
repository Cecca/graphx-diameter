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

import org.scalatest.{FreeSpec, Matchers}

import scala.util.Random

class PriorityQueueSpec extends FreeSpec with Matchers {

  "The queue must respect priority" - {

    "Small queue" in {
      val pq = new PriorityQueue(4)

      pq.enqueue(0,1)
      pq.enqueue(1,2)
      pq.enqueue(2,3)
      pq.enqueue(3,0)

      pq.dequeue() should be (3)
      pq.dequeue() should be (0)
      pq.dequeue() should be (1)
      pq.dequeue() should be (2)
    }

    "Big queue" in {

      val elems = Vector(
        (86,4),
        (90,1),
        (1,3),
        (10,6),
        (14,2),
        (56,5),
        (60,0),
        (99,7)
      )

      val pq = new PriorityQueue(elems.size)

      for((pri, v) <- elems) {
        pq.enqueue(v, pri)
        pq.heap should contain (v)
        pq.weight should contain (pri)
      }
      pq.isHeap(1) should be (true)

      pq.dequeue() should be (3)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (6)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (2)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (5)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (0)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (4)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (1)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (7)
      pq.isHeap(1) should be (true)

    }
  }

  "Helper methods" in {

    val pq = new PriorityQueue(4)

    pq.empty should be (true)
    pq.nonEmpty should be (false)

    pq.enqueue(0, 1)

    pq.empty should be (false)
    pq.nonEmpty should be (true)

    pq.dequeue()

    pq.empty should be (true)
    pq.nonEmpty should be (false)
  }

  "Random queue" in {
    val dim = Random.nextInt(80)
    // vector of random pairs with no duplicate keys.
    // Duplicate keys are no problem for the priority queue (the dequeue order is not relevant),
    // but are a problem for the test. Hence we remove them.
    val vals = Range(0, dim).map(i => (Random.nextInt(100), i)).toMap.toVector
    val pq = new PriorityQueue(dim)

    for((pri, v) <- vals) {
      pq.enqueue(v, pri)
    }

    for((pri, v) <- vals.sortBy(_._1)) {
      pq.dequeue() should equal (v)
    }

  }

}
