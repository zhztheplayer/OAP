package com.intel.sparkColumnarPlugin.expression

import com.google.common.collect.Lists
import scala.collection.immutable.List
import scala.collection.JavaConverters._
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._

trait ColumnarAggregateExpressionBase extends ColumnarExpression with Logging {
  def getFieldName: String
  def getResultFieldName: String
  def getField: Field
  def getResultField: Field
  def doColumnarCodeGen_ext(args: Object): (TreeNode, TreeNode) = {
    throw new UnsupportedOperationException(s"ColumnarAggregateExpressionBase doColumnarCodeGen_ext is a abstract function.")
  }
}

class ColumnarUniqueAggregateExpression(aggrField: Field, resField: Field) extends ColumnarAggregateExpressionBase with Logging {

  override def getFieldName: String = {
    aggrField.getName()
  }

  override def getField: Field = {
    aggrField
  }

  override def getResultFieldName: String = {
    resField.getName
  }

  override def getResultField: Field = {
    resField
  }

  override def doColumnarCodeGen_ext(args: Object): (TreeNode, TreeNode) = {
    val (keyFieldList, inputFieldList, resultType, resultField) =
      args.asInstanceOf[(List[Field], List[Field], ArrowType, Field)]
    val funcName = "action_unique"
    val inputFieldNode = {
      // if keyList has keys, we need to do groupby by these keys.
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava,
        resultType/*this arg won't be used*/)
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayListWithAction",
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava,
        resultType/*this arg won't be used*/)
      Lists.newArrayList(groupByFuncNode, TreeBuilder.makeField(aggrField))
    }
    val aggregateNode = TreeBuilder.makeFunction(
        funcName,
        inputFieldNode,
        resultType)
    (aggregateNode, null)
  }
}

class ColumnarAggregateExpression(
    aggrField: Field,
    resField: Field,
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean,
    resultId: ExprId)
    extends AggregateExpression(aggregateFunction, mode, isDistinct, resultId)
    with ColumnarAggregateExpressionBase
    with Logging {

  override def getFieldName: String = {
    aggrField.getName()
  }

  override def getField: Field = {
    aggrField
  }

  override def getResultFieldName: String = {
    resField.getName
  }

  override def getResultField: Field = {
    resField
  }

  override def doColumnarCodeGen_ext(args: Object): (TreeNode, TreeNode) = {
    val (keyFieldList, inputFieldList, resultType, resultField) =
      args.asInstanceOf[(List[Field], List[Field], ArrowType, Field)]
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
    val (aggregateNode, finalFuncNode) = if (keyFieldList.isEmpty != true) {
      // if keyList has keys, we need to do groupby by these keys.
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava,
        resultType/*this arg won't be used*/)
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayListWithAction",
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava,
        resultType/*this arg won't be used*/)
      val inputFieldNode = Lists.newArrayList(groupByFuncNode, TreeBuilder.makeField(aggrField))
      val aggregateFuncName = "action_" + funcName
      (TreeBuilder.makeFunction(
        aggregateFuncName,
        inputFieldNode,
        resultType), null)
    } else {
      val inputFieldNode = Lists.newArrayList(TreeBuilder.makeField(aggrField))
      (TreeBuilder.makeFunction(
        funcName,
        inputFieldNode,
        resultType),
      TreeBuilder.makeFunction(
        finalFuncName,
        inputFieldNode,
        resultType))
    }
    (aggregateNode, finalFuncNode)
  }
}
