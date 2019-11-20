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
  def getField: Field
  def getFieldType: ArrowType
  def doColumnarCodeGen_ext(args: Object): (TreeNode, TreeNode) = {
    throw new UnsupportedOperationException(s"ColumnarAggregateExpressionBase doColumnarCodeGen_ext is a abstract function.")
  }
}

class ColumnarUniqueAggregateExpression(aggrField: Field) extends ColumnarAggregateExpressionBase with Logging {

  override def getFieldName: String = {
    aggrField.getName()
  }

  override def getFieldType: ArrowType = {
    aggrField.getType()
  }

  override def getField: Field = {
    aggrField
  }

  override def doColumnarCodeGen_ext(args: Object): (TreeNode, TreeNode) = {
    val (keyFieldList, inputFieldList, resultType, resultField) =
      args.asInstanceOf[(List[Field], List[Field], ArrowType, Field)]
    val funcName = "unique"
    val inputFieldNode = {
      // if keyList has keys, we need to do groupby by these keys.
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava,
        resultType/*this arg won't be used*/)
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayList",
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava,
        resultType/*this arg won't be used*/)
      Lists.newArrayList(groupByFuncNode, TreeBuilder.makeField(aggrField))
    }
    val aggregateNode = TreeBuilder.makeFunction(
        funcName,
        inputFieldNode,
        resultType)
    val cacheNode = TreeBuilder.makeFunction(
        "appendToCachedArray",
        Lists.newArrayList(aggregateNode),
        resultType)
    val finalNode = TreeBuilder.makeFunction(
        funcName,
        Lists.newArrayList(),
        resultType)
    (cacheNode, finalNode)
  }
}

class ColumnarAggregateExpression(
    aggrField: Field,
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

  override def getFieldType: ArrowType = {
    aggrField.getType()
  }

  override def getField: Field = {
    aggrField
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
    val inputFieldNode = if (keyFieldList.isEmpty != true) {
      // if keyList has keys, we need to do groupby by these keys.
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava,
        resultType/*this arg won't be used*/)
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayList",
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava,
        resultType/*this arg won't be used*/)
      Lists.newArrayList(groupByFuncNode, TreeBuilder.makeField(aggrField))
    } else {
      Lists.newArrayList(TreeBuilder.makeField(aggrField))
    }
    val aggregateNode = TreeBuilder.makeFunction(
        funcName,
        inputFieldNode,
        resultType)
    val cacheNode = TreeBuilder.makeFunction(
        "appendToCachedArray",
        Lists.newArrayList(aggregateNode),
        resultType)
    val finalNode = TreeBuilder.makeFunction(
        finalFuncName,
        Lists.newArrayList(),
        resultType)
    (cacheNode, finalNode)
  }
}
