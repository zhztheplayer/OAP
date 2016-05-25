package org.apache.spark.sql.execution.datasources.spinach.utils

object IndexUtils {
  def readIntFromByteArray(bytes: Array[Byte], offset: Int): Int = {
    bytes(3 + offset) & 0xFF | (bytes(2 + offset) & 0xFF) << 8 |
      (bytes(1 + offset) & 0xFF) << 16 | (bytes(offset) & 0xFF) << 24
  }
}
