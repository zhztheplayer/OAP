package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarLiteral(lit: Literal) extends Literal(lit.value, lit.dataType) with ColumnarExpression {

  override def doColumnarCodeGen(fieldTypes: List[Field]): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    dataType match {
      case t: StringType =>
        (TreeBuilder.makeStringLiteral(value.asInstanceOf[String]), resultType)
      case t: IntegerType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[Integer]), resultType)
    }
  }
}

