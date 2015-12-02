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

import scala.annotation.tailrec

/**
 * Implementation of a fixed max-size priority queue to be used in
 * Dijkstra's algorithm. Internally it uses a min-heap.
 *
 * Can store values in the range [0..maxSize-1].
 *
 * @param maxSize the maximum size
 */
private[diameter]
class PriorityQueue(val maxSize: Int) {

  val heap = Array.fill[Int](maxSize + 1)(-1)

  val weight = Array.fill[Distance](maxSize)(Infinity)

  val position = Array.fill(maxSize)(-1)

  private var _size = 0

  def empty: Boolean = _size < 1

  def nonEmpty: Boolean = !empty

  def parent(i: Int): Int = i / 2

  def right(i: Int): Int = 2*i + 1

  def left(i: Int): Int = 2*i

  def isHeap(i: Int): Boolean = {
    if(i > _size) {
      true
    } else if((left(i) <= _size &&  weight(heap(i)) > weight(heap(left(i)))) ||
      (right(i) <= _size && weight(heap(i)) > weight(heap(right(i))))) {
      false
    } else {
      isHeap(left(i)) && isHeap(right(i))
    }
  }

  @tailrec
  final def heapify(i: Int): Unit = {
    val l = left(i)
    val r = right(i)

    var smallest =
      if (l <= _size && weight(heap(l)) < weight(heap(i))) {
        l
      } else {
        i
      }
    smallest =
      if (r <= _size && weight(heap(r)) < weight(heap(smallest))) {
        r
      } else {
        smallest
      }

    if (smallest != i){
      val rootVal = heap(i)
      val childVal = heap(smallest)
      heap(i) = childVal
      heap(smallest) = rootVal
      position(childVal) = i
      position(rootVal) = smallest
      heapify(smallest)
    }
  }

  def enqueue(x: Int, w: Distance): Unit = {
    if(x >= maxSize) {
      throw new IllegalArgumentException(
        "Can store only elements less than the maximum heap size")
    }

    _size += 1
    heap(_size) = x
    position(x) = _size
    weight(x) = Infinity
    decreasePriority(x, w)
  }

  def dequeue(): Int = {
    if(_size < 1) {
      throw new IndexOutOfBoundsException("Cannot dequeue an empty queue")
    }

    val min = heap(1)
    heap(1) = heap(_size)
    position(heap(1)) = 1
    heap(_size) = -1 // TODO remove this, not necessary
    _size -= 1
    position(min) = -1
    heapify(1)
    min
  }

  def decreasePriority(x: Int, w: Distance) = {
    if(w > weight(x)) {
      throw new IllegalArgumentException(
        "The new weight is greater than the previous weight")
    }
    if(position(x) < 0) {
      throw new IllegalArgumentException(
        s"Cannot decrease priority of an element not in the queue: $x")
    }

    weight(x) = w
    var i = position(x)
    var par = parent(i)
    while(i > 1 && weight(heap(par)) > w) {
      val parVal = heap(par)
      heap(i) = parVal
      heap(par) = x
      position(parVal) = i
      position(x) = par
      i = parent(i)
      par = parent(i)
    }
  }

  override def toString: String =
    s"""
      | Heap      = ${heap.tail.toList.map(x => f"$x%4d")}
      | Weights   = ${heap.tail.toList.map(x => if(x>=0) f"${weight(x)}%4f" else "  -1")}
      | Positions = ${position.toList.map(x => f"$x%4d")}
    """.stripMargin

}
