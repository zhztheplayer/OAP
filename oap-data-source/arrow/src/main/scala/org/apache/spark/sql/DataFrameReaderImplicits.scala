package org.apache.spark.sql

class DataFrameReaderImplicits(r: DataFrameReader) {

  /**
   * Loads an file via Arrow Datasets API and returns the result as a `DataFrame`.
   *
   * @param path input path
   * @since 3.0.0-SNAPSHOT
   */
  def arrow(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    arrow(Seq(path): _*)
  }

  /**
   * Loads files via Arrow Datasets API and returns the result as a `DataFrame`.
   *
   * @param paths input paths
   * @since 3.0.0-SNAPSHOT
   */
  @scala.annotation.varargs
  def arrow(paths: String*): DataFrame = r.format("arrow").load(paths: _*)
}

object DataFrameReaderImplicits {
  implicit def readerConverter(r: DataFrameReader): DataFrameReaderImplicits = {
    new DataFrameReaderImplicits(r)
  }
}
