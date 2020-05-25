package com.intel.sparkColumnarPlugin.expression

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class ColumnarIf(predicate: Expression, trueValue: Expression,
                 falseValue: Expression, original: Expression)
    extends If(predicate: Expression, trueValue: Expression, falseValue: Expression)
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
      val (predicate_node, predicateType): (TreeNode, ArrowType) =
      predicate.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (true_node, trueType): (TreeNode, ArrowType) =
        trueValue.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
      val (false_node, falseType): (TreeNode, ArrowType) =
        falseValue.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

      val funcNode = TreeBuilder.makeIf(predicate_node, true_node, false_node, trueType)
      (funcNode, trueType)
  }
}

object ColumnarIfOperator {

  def create(predicate: Expression, trueValue: Expression,
             falseValue: Expression, original: Expression): Expression = original match {
    case i: If =>
      new ColumnarIf(predicate, trueValue, falseValue, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
