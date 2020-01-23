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
  def setField(inField: Field, outField: Field): Unit
  def doColumnarCodeGen_ext(args: Object): (TreeNode, TreeNode) = {
    throw new UnsupportedOperationException(s"ColumnarAggregateExpressionBase doColumnarCodeGen_ext is a abstract function.")
  }
}

class ColumnarUniqueAggregateExpression() extends ColumnarAggregateExpressionBase with Logging {

  var aggrField : Field = _
  var resField : Field = _

  override def setField(inField: Field, outField: Field): Unit = {
    aggrField = inField
    resField = outField
  }

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
    val inputNode = {
      // if keyList has keys, we need to do groupby by these keys.
      var inputFieldNode = 
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava
      logInfo(s"encodeArray($keyFieldList)")
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      inputFieldNode = 
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava
      logInfo(s"splitArrayListWithAction($inputFieldList)")
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayListWithAction",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      Lists.newArrayList(groupByFuncNode, TreeBuilder.makeField(aggrField))
    }
    logInfo(s"$funcName($inputNode)")
    val aggregateNode = TreeBuilder.makeFunction(
        funcName,
        inputNode,
        resultType)
    (aggregateNode, null)
  }
}

class ColumnarAggregateExpression(
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean,
    resultId: ExprId)
    extends AggregateExpression(aggregateFunction, mode, isDistinct, resultId)
    with ColumnarAggregateExpressionBase
    with Logging {

  var aggrField : Field = _
  var resField : Field = _

  override def setField(inField: Field, outField: Field): Unit = {
    aggrField = inField
    resField = outField
  }

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
      var inputFieldNode = 
        keyFieldList.map({field => TreeBuilder.makeField(field)}).asJava
      logInfo(s"encodeArray($keyFieldList)")
      val encodeNode = TreeBuilder.makeFunction(
        "encodeArray",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      inputFieldNode = 
        (encodeNode :: inputFieldList.map({field => TreeBuilder.makeField(field)})).asJava
      logInfo(s"splitArrayListWithAction($inputFieldList)")
      val groupByFuncNode = TreeBuilder.makeFunction(
        "splitArrayListWithAction",
        inputFieldNode,
        resultType/*this arg won't be used*/)
      inputFieldNode = 
        Lists.newArrayList(groupByFuncNode, TreeBuilder.makeField(aggrField))
      val aggregateFuncName = "action_" + funcName
      logInfo(s"$aggregateFuncName($aggrField)")
      (TreeBuilder.makeFunction(
        aggregateFuncName,
        inputFieldNode,
        resultType), null)
    } else {
      val inputFieldNode = Lists.newArrayList(TreeBuilder.makeField(aggrField))
      logInfo(s"$funcName($inputFieldNode)")
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
