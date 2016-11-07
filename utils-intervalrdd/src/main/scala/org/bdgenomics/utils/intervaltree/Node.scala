/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.utils.intervaltree

import java.io.Serializable
import scala.reflect.ClassTag

protected class Node[K <: Interval[K], T: ClassTag](init: Array[(K, T)]) extends Serializable {

  /* left and right children of this node */
  var leftChild: Node[K, T] = null
  var rightChild: Node[K, T] = null

  /* stores data values for this node */
  var data: Array[(K, T)] = init

  def interval = data.map(_._1).reduce((k1, k2) => k1.hull(k2))

  /* maximum end that is seen in this node. used for search */
  var subtreeMax: Long = interval.end

  /* returns interval for this node */
  def getInterval: K = interval

  /* alternative constructor of node from data */
  def this(kv: (K, T)) = {
    this(Array(kv))
  }

  /* gets the number of data values in this node */
  def getSize(): Long = {
    data.length
  }

  /* clones node */
  override def clone: Node[K, T] = new Node(data)

  /* resets left and right child to null */
  def clearChildren() = {
    leftChild = null
    rightChild = null
  }

  /**
   * Puts multiple elements in data array
   *
   * @param rs: Array of elements to place in node
   */
  def multiput(rs: Array[(K, T)]): Unit = {
    val newData = rs
    data ++= newData
  }

  /**
   * Puts multiple elements in data array
   *
   * @param rs: Iterator of elements to place in node
   */
  def multiput(rs: Iterator[(K, T)]): Unit = {
    multiput(rs.toArray)
  }

  /**
   * Puts element in data array
   *
   * @param r: Element to place in node
   */
  def put(r: (K, T)) = {
    multiput(Array(r))
  }

  /**
   * Gets all elements in node
   * @return Iterator of (key, value) elements in node
   */
  def get(): Iterator[(K, T)] = data.toIterator

  /**
   * checks whether this node is greater than other Interval K
   * @return Boolean whether this interval > other
   */
  def greaterThan(other: K): Boolean = {
    interval.mid - other.mid > 0
  }
  /**
   * checks whether this node equals other Interval K
   * @return Boolean whether this interval == other
   */
  def equals(other: K): Boolean = {
    (interval.start == other.start && interval.end == other.end)
  }

  def aboutEquals(other: K): Boolean = {
    math.abs(interval.mid - other.mid) < Node.threshold
  }

  /**
   * checks whether this node is less than other Interval K
   * @return Boolean whether this interval < other
   */
  def lessThan(other: K): Boolean = {
    interval.mid - other.mid < 0
  }

  /**
   * checks whether this node overlaps Interval K
   * @return Boolean whether this interval overlaps other
   */
  def overlaps(other: K): Boolean = {
    interval.start < other.end && interval.end > other.start
  }
}

object Node {
  var threshold: Int = 1000
}
