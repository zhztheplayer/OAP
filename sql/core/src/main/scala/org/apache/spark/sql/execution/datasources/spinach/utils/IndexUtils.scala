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

package org.apache.spark.sql.execution.datasources.spinach.utils

import java.io.DataOutputStream

import org.apache.spark.sql.execution.datasources.spinach.FiberCacheData
import org.apache.spark.unsafe.Platform

object IndexUtils {
  def readIntFromByteArray(bytes: Array[Byte], offset: Int): Int = {
    bytes(3 + offset) & 0xFF | (bytes(2 + offset) & 0xFF) << 8 |
      (bytes(1 + offset) & 0xFF) << 16 | (bytes(offset) & 0xFF) << 24
  }

  def writeInt(out: DataOutputStream, v: Int): Unit = {
    out.write((v >>>  0) & 0xFF)
    out.write((v >>>  8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
  }

  def readIntFromUnsafe(bytes: FiberCacheData, offset: Int): Int = {
    val baseObj = bytes.fiberData.getBaseObject
    val baseOff = bytes.fiberData.getBaseOffset
    val byte3 = Platform.getByte(baseObj, baseOff + offset + 3)
    val byte2 = Platform.getByte(baseObj, baseOff + offset + 2)
    val byte1 = Platform.getByte(baseObj, baseOff + offset + 1)
    val byte0 = Platform.getByte(baseObj, baseOff + offset + 0)
    byte3 & 0xFF | (byte2 & 0xFF) << 8 | (byte1 & 0xFF) << 16 | (byte0 & 0xFF) << 24
  }
}
