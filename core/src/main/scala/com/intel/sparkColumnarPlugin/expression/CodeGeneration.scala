package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.types._

object CodeGeneration {

  def getResultType(left: ArrowType, right: ArrowType): ArrowType = {
    if (left.equals(right)) {
      left
    } else {
      throw new UnsupportedOperationException(
        s"getResultType left is $left, right is $right, not equal.")
    }
  }

  def getResultType(dataType: DataType): ArrowType = dataType match {
    case t: IntegerType =>
      new ArrowType.Int(32, true)
    case l: LongType =>
      new ArrowType.Int(64, true)
    case t: FloatType =>
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case d: DecimalType =>
      new ArrowType.Decimal(d.precision, d.scale)
    case other =>
      throw new UnsupportedOperationException(s"getResultType doesn't support $other.")
  }
}
