package com.intel.sparkColumnarPlugin

import org.apache.spark.SparkConf

class ColumnarPluginConfig(conf: SparkConf) {
  val enableColumnarSort: Boolean =
    conf.getBoolean("spark.sql.columnar.sort", defaultValue = false)
}

object ColumnarPluginConfig {
  var ins: ColumnarPluginConfig = null
  def getConf(conf: SparkConf): ColumnarPluginConfig = synchronized {
    if (ins == null) {
      ins = new ColumnarPluginConfig(conf)
      ins
    } else {
      ins
    }
  }
  def getConf: ColumnarPluginConfig = synchronized {
    if (ins == null) {
      throw new IllegalStateException("ColumnarPluginConfig is not initialized yet")
    } else {
      ins
    }
  }
}
