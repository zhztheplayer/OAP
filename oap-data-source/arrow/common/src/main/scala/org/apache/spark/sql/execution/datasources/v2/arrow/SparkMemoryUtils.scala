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

import org.apache.arrow.memory.{AllocationListener, BaseAllocator, BufferAllocator, OutOfMemoryException}

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.util.TaskCompletionListener

object SparkMemoryUtils {
  private val taskToAllocatorMap = new java.util.IdentityHashMap[TaskContext, BufferAllocator]()

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

  def arrowAllocator(): BaseAllocator = {
    if (!inSparkTask()) {
      return org.apache.spark.sql.util.ArrowUtils.rootAllocator
    }
    val tc = getLocalTaskContext
    val allocator = taskToAllocatorMap.synchronized {
      if (taskToAllocatorMap.containsKey(tc)) {
        taskToAllocatorMap.get(tc).asInstanceOf[BaseAllocator]
      } else {
        val al = new ExecutionMemoryAllocationListener(getTaskMemoryManager())
        val parent = org.apache.spark.sql.util.ArrowUtils.rootAllocator
        val newInstance = parent.newChildAllocator("Spark Managed Allocator - " +
          UUID.randomUUID().toString, al, 0, parent.getLimit).asInstanceOf[BaseAllocator]
        taskToAllocatorMap.put(tc, newInstance)
        getLocalTaskContext.addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            taskToAllocatorMap.synchronized {
              if (taskToAllocatorMap.containsKey(context)) {
                try {
                  taskToAllocatorMap.get(context).close()
                } catch {
                  case t: Throwable =>
                    SparkContext.getActive.foreach {
                      sc =>
                        val conf = sc.getConf
                        if (conf.get("spark.unsafe.exceptionOnMemoryLeak").toBoolean) {
                          throw t
                        } else {
                          return
                        }
                    }
                    throw t
                } finally {
                  taskToAllocatorMap.remove(context)
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
}
