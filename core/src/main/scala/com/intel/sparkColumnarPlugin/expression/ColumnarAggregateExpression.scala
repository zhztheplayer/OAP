package com.intel.sparkColumnarPlugin.expression

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
  with ColumnarExpression with Logging {
  def doColumnarCodeGen_ext(args: Object): (TreeNode, ArrowType, TreeNode) = {
    val (inputFieldList, outpurAttr) = args.asInstanceOf[(List[Field], Attribute)]

    val funcName = mode match {
      case Partial => aggregateFunction.prettyName 
      case Final => aggregateFunction.prettyName match {
        case "count" => "sum"
        case other => aggregateFunction.prettyName
      }
    }

    val finalFuncName = funcName match {
      case "count" => "sum"
      case other => other
    }
    logInfo(s"funcName is $funcName, finalFuncName is $finalFuncName, mode is $mode")
    val colName = outpurAttr.name
    val resultType = CodeGeneration.getResultType(outpurAttr.dataType)
    val fieldNodeList = inputFieldList.map(TreeBuilder.makeField(_))
    (TreeBuilder.makeFunction(funcName, fieldNodeList.asJava, resultType),
     resultType,
     TreeBuilder.makeFunction(finalFuncName, fieldNodeList.asJava, resultType))
  }
}
