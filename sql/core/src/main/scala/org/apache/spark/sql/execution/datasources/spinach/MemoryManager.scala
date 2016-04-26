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

import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}

/**
  * Used to cache data in MemoryBlock (on-heap or off-heap)
  */
case class FiberCacheData(fiberData: MemoryBlock)

private[spinach] trait MemoryManager {
  def maxMemoryInByte: Long
  def allocate(numOfBytes: Int): FiberCacheData
  def free(fiber: FiberCacheData)
  def remain(): Long
}

private[spinach] trait MemoryMode
private[spinach] case object OffHeap extends MemoryMode
private[spinach] case object OnHeap extends MemoryMode

private[spinach] class CommonMemoryManager(var maxMemoryInByte: Long, var memoryMode: MemoryMode)
  extends MemoryManager {

  def getMemoryMode: MemoryMode = memoryMode

  def setMemoryMode(memoryMode: MemoryMode): Unit = {
    this.memoryMode = memoryMode
  }

  def allocate(numOfBytes: Int): FiberCacheData = synchronized {
    if (maxMemoryInByte - numOfBytes >= 0) {
      maxMemoryInByte -= numOfBytes
      val fiberData = memoryMode match {
        case OnHeap => MemoryAllocator.HEAP.allocate(numOfBytes)
        case OffHeap => MemoryAllocator.UNSAFE.allocate(numOfBytes)
        case _ => MemoryAllocator.HEAP.allocate(numOfBytes)
      }
      FiberCacheData(fiberData)
    } else {
      null
    }
  }

  def free(fiber: FiberCacheData): Unit = synchronized {
    memoryMode match {
      case OnHeap => MemoryAllocator.HEAP.free(fiber.fiberData)
      case OffHeap => MemoryAllocator.UNSAFE.free(fiber.fiberData)
      case _ => MemoryAllocator.HEAP.free(fiber.fiberData)
    }
    maxMemoryInByte += fiber.fiberData.size()
  }

  def remain(): Long = maxMemoryInByte
}

private[spinach] object MemoryManager {
  // TODO make it configurable
  val SPINACH_FIBER_CACHE_SIZE_IN_BYTES: Long = 1024 * 1024 * 1024L // 1GB
  val SPINACH_DATA_META_CACHE_SIZE: Int = 1024
  val MEMORY_MODE: MemoryMode = OnHeap

  val instance: MemoryManager =
    new CommonMemoryManager(SPINACH_FIBER_CACHE_SIZE_IN_BYTES, MEMORY_MODE)
}
