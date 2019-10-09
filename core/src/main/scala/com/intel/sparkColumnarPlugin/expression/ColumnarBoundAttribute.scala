package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.mutable.ListBuffer

class ColumnarBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends BoundReference(ordinal, dataType, nullable) with ColumnarExpression {

  override def doColumnarCodeGen(fieldTypes: List[Field]): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    val field = Field.nullable(s"c_$ordinal", resultType)
    var found = false
    for (f <- fieldTypes) {
      if (found || f.equals(field)) {
        found = true
      } else {
        found = false
      }
    }
    if (found)
      (TreeBuilder.makeField(field), resultType)
    else
      (null, null)
  }

}
