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

import scala.collection.mutable.ListBuffer

class ColumnarInSet(value: Expression, hset: Set[Any], original: Expression)
    extends InSet(value: Expression, hset: Set[Any])
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (value_node, valueType): (TreeNode, ArrowType) =
      value.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()
    if (value.dataType == StringType) {
      val newlist = hset.toList.map (expr => {
        expr.toString
      });
      val tlist = Lists.newArrayList(newlist:_*);
      val funcNode = TreeBuilder.makeInExpressionString(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else if (value.dataType == IntegerType) {
      val newlist = hset.toList.map (expr => {
        expr.asInstanceOf[Integer]
      });
      val tlist = Lists.newArrayList(newlist:_*);
      val funcNode = TreeBuilder.makeInExpressionInt32(value_node: TreeNode, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else if (value.dataType == LongType) {
      val newlist = hset.toList.map (expr => {
        expr.asInstanceOf[java.lang.Long]
      });
      val tlist = Lists.newArrayList(newlist:_*);
      val funcNode = TreeBuilder.makeInExpressionBigInt(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else {
      throw new UnsupportedOperationException(s"not currently supported: ${value.dataType}.")
    }
  }
}

object ColumnarInSetOperator {

  def create(value: Expression, hset: Set[Any], original: Expression): Expression = original match {
    case i: InSet =>
      new ColumnarInSet(value, hset, i)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
