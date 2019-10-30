package com.intel.sparkColumnarPlugin.expression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
object ColumnarExpressionConverter extends Logging {

  var check_if_no_calculation = true

  def replaceWithColumnarExpression(expr: Expression): Expression = expr match {
    case a: Alias =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarAlias(replaceWithColumnarExpression(a.child), a.name)(
        a.exprId,
        a.qualifier,
        a.explicitMetadata)
    case lit: Literal =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarLiteral(lit)
    case binArith: BinaryArithmetic =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarBinaryArithmetic.create(
        replaceWithColumnarExpression(binArith.left),
        replaceWithColumnarExpression(binArith.right),
        expr)
    case b: BoundReference =>
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
    case b: BinaryOperator =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarBinaryOperator.create(
        replaceWithColumnarExpression(b.left),
        replaceWithColumnarExpression(b.right),
        expr)
    case u: UnaryExpression =>
      if (!u.isInstanceOf[Cast]) {
        check_if_no_calculation = false
      }
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      ColumnarUnaryOperator.create(replaceWithColumnarExpression(u.child), expr)
    case a: AggregateExpression =>
      check_if_no_calculation = false
      logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
      new ColumnarAggregateExpression(a.aggregateFunction, a.mode, a.isDistinct, a.resultId)
    case expr =>
      logWarning(s"${expr.getClass} ${expr} is not currently supported.")
      expr
  }

  def ifNoCalculation = check_if_no_calculation

  def reset(): Unit = {
    check_if_no_calculation = true
  }

}
