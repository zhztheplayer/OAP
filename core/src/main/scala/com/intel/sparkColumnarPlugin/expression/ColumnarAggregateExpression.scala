package com.intel.sparkColumnarPlugin.expression

import com.google.common.collect.Lists
import scala.collection.JavaConverters._
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

class ColumnarAggregateExpression(
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean,
    resultId: ExprId)
    extends AggregateExpression(aggregateFunction, mode, isDistinct, resultId)
    with ColumnarExpression
    with Logging {
  def doColumnarCodeGen_ext(args: Object): (TreeNode, Field, TreeNode) = {
    val (inputField, outpurAttr, resultName) = args.asInstanceOf[(Field, Attribute, String)]

    val funcName = mode match {
      case Partial => aggregateFunction.prettyName
      case Final =>
        aggregateFunction.prettyName match {
          case "count" => "sum"
          case other => aggregateFunction.prettyName
        }
      case _ =>
        throw new UnsupportedOperationException("doesn't support this mode")
    }

    val finalFuncName = funcName match {
      case "count" => "sum"
      case other => other
    }
    logInfo(s"funcName is $funcName, finalFuncName is $finalFuncName, mode is $mode")
    val resultType = CodeGeneration.getResultType(outpurAttr.dataType)
    val resultFieldNode = Field.nullable(resultName, resultType)
    (
      TreeBuilder.makeFunction(
        funcName,
        Lists.newArrayList(TreeBuilder.makeField(inputField)),
        resultType),
      resultFieldNode,
      TreeBuilder.makeFunction(
        finalFuncName,
        Lists.newArrayList(TreeBuilder.makeField(resultFieldNode)),
        resultType))
  }
}
