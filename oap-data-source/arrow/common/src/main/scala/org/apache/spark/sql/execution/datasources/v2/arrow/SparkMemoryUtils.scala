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

package org.apache.spark.sql.execution.datasources.v2.arrow

import java.util.UUID

import com.intel.oap.spark.sql.execution.datasources.v2.arrow.SparkManagedReservationListener
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.memory.{AllocationListener, BaseAllocator, BufferAllocator, DirectReservationListener, OutOfMemoryException, ReservationListener, RootAllocator}

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.util.TaskCompletionListener

object SparkMemoryUtils {

  private val rootAllocator = new RootAllocator(
    BaseAllocator.configBuilder()
      .allocationManagerFactory(UnsafeAllocationManager1.FACTORY)
      .build())

  private val taskToAllocatorMap = new java.util.IdentityHashMap[TaskContext, BufferAllocator]()
  private val taskToMemoryPoolMap =
    new java.util.IdentityHashMap[TaskContext, NativeMemoryPool]()

  private class ExecutionMemoryAllocationListener(mm: TaskMemoryManager)
    extends MemoryConsumer(mm, mm.pageSizeBytes(), MemoryMode.OFF_HEAP) with AllocationListener {

    override def onPreAllocation(size: Long): Unit = {
      if (size == 0) {
        return
      }
      val granted = acquireMemory(size)
      if (granted < size) {
        throw new OutOfMemoryException("Not enough spark off-heap execution memory. " +
          "Acquired: " + size + ", granted: " + granted + ". " +
          "Try tweaking config option spark.memory.offHeap.size to " +
          "get larger space to run this application. ")
      }
    }

    override def onRelease(size: Long): Unit = {
      freeMemory(size)
    }

    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      // not spillable
      0L
    }
  }

  private def getLocalTaskContext: TaskContext = TaskContext.get()

  private def getTaskMemoryManager(): TaskMemoryManager = {
    getLocalTaskContext.taskMemoryManager()
  }

  private def inSparkTask(): Boolean = {
    getLocalTaskContext != null
  }

  def addLeakSafeTaskCompletionListener[U](f: TaskContext => U): TaskContext = {
    contextMemoryPool()
    contextAllocator()
    getLocalTaskContext.addTaskCompletionListener(f)
  }

  def globalAllocator(): BaseAllocator = {
    rootAllocator
  }

  def contextAllocator(): BaseAllocator = {
    val globalAlloc = globalAllocator()
    if (!inSparkTask()) {
      return globalAlloc
    }
    val tc = getLocalTaskContext
    val allocator = taskToAllocatorMap.synchronized {
      if (taskToAllocatorMap.containsKey(tc)) {
        taskToAllocatorMap.get(tc).asInstanceOf[BaseAllocator]
      } else {
        val al = new ExecutionMemoryAllocationListener(getTaskMemoryManager())
        val parent = globalAlloc
        val newInstance = parent.newChildAllocator("Spark Managed Allocator - " +
          UUID.randomUUID().toString, al, 0, parent.getLimit).asInstanceOf[BaseAllocator]
        taskToAllocatorMap.put(tc, newInstance)
        getLocalTaskContext.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              taskToAllocatorMap.synchronized {
                if (taskToAllocatorMap.containsKey(context)) {
                  val allocator = taskToAllocatorMap.get(context)
                  val allocated = allocator.getAllocatedMemory
                  if (allocated == 0L) {
                    close(allocator)
                    taskToAllocatorMap.remove(context)
                  } else {
                    softClose(allocator)
                  }
                }
              }
            }
          })
        newInstance
      }
    }
    allocator
  }

  private def close(allocator: BufferAllocator): Unit = {
    allocator.getChildAllocators.forEach(close(_))
    allocator.close()
  }

  /**
   * Close the allocator quietly without having any OOM errors thrown. We rely on Spark's memory
   * management system to detect possible memory leaks after the task get successfully down. Any
   * leak shown right here is possibly not actual because buffers may be cleaned up after
   * this check code is executed. Having said that developers should manage to make sure
   * the specific clean up logic of operators is registered at last of the program which means
   * it will be executed earlier.
   *
   * @see org.apache.spark.executor.Executor.TaskRunner#run()
   */
  private def softClose(allocator: BufferAllocator): Unit = {
    // do nothing
  }

  def globalMemoryPool(): NativeMemoryPool = {
    NativeMemoryPool.getDefault
  }

  def contextMemoryPool(): NativeMemoryPool = {
    if (!inSparkTask()) {
      return globalMemoryPool()
    }
    val tc = getLocalTaskContext
    val pool = taskToMemoryPoolMap.synchronized {
      if (taskToMemoryPoolMap.containsKey(tc)) {
        taskToMemoryPoolMap.get(tc)
      } else {
        val rl = new SparkManagedReservationListener(getTaskMemoryManager())
        val pool = NativeMemoryPool.createListenable(rl)
        taskToMemoryPoolMap.put(tc, pool)
        getLocalTaskContext.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              taskToMemoryPoolMap.synchronized {
                if (taskToMemoryPoolMap.containsKey(context)) {
                  val pool = taskToMemoryPoolMap.get(context)
                  val allocated = pool.getBytesAllocated()
                  if (allocated == 0L) {
                    taskToMemoryPoolMap.remove(context).close()
                  } else {
                    // do nothing
                  }
                }
              }
            }
          })
        pool
      }
    }
    pool
  }
}
