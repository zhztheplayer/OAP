/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.spinach

/**
 * Util functions for building BTree from sorted iterator
 */
object BTreeUtils {
  private val BRANCHING: Int = 5

  /**
   * Splits an array of n
   */
  private def shape(n: Long, height: Int): Seq[Int] = {
    if (height == 1) {
      return Seq(n.toInt)
    }
    val maxSubTree = math.pow(BRANCHING, height - 1).toLong
    val numOfSubTree = (if (n % maxSubTree == 0) n / maxSubTree else n / maxSubTree + 1).toInt
    val baseSubTreeSize = n / numOfSubTree
    val remainingSize = (n % numOfSubTree).toInt
    (1 to numOfSubTree).map(i => baseSubTreeSize + (if (i <= remainingSize) 1 else 0)).flatMap(
      subSize => shape(subSize, height - 1)
    )
  }

  /**
   * Estimates height of BTree.
   */
  private def height(size: Long): Int = {
    math.ceil(math.log(size + 1) / math.log(BRANCHING)).toInt
  }

  /**
   * Generates a B Tree size sequence from total size.
   */
  def generate(n: Long): Seq[Int] = {
    shape(n, height(n))
  }
}
