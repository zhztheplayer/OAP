package com.intel.sparkColumnarPlugin.expression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
object ColumnarExpressionConverter extends Logging {

  def replaceWithColumnarExpression(expr: Expression): Expression = expr match {
    case a: Alias =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarAlias(replaceWithColumnarExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case lit: Literal =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarLiteral(lit)
    case binArith: BinaryArithmetic =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      ColumnarBinaryArithmetic.create(replaceWithColumnarExpression(binArith.left), replaceWithColumnarExpression(binArith.right), expr)
    case b: BoundReference =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
    case b: BinaryOperator =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      ColumnarBinaryOperator.create(replaceWithColumnarExpression(b.left), replaceWithColumnarExpression(b.right), expr)
    case u: UnaryExpression =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      ColumnarUnaryOperator.create(replaceWithColumnarExpression(u.child), expr)
    case a: AggregateExpression =>
      logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarAggregateExpression(a.aggregateFunction, a.mode, a.isDistinct, a.resultId)
    case expr =>
      logWarning(s"${expr.getClass} ${expr} is not currently supported.")
      expr
  }

}
