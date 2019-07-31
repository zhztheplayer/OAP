package com.intel.sparkColumnarPlugin.expression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
object ColumnarExpressionConverter extends Logging {

  def replaceWithColumnarExpression(expr: Expression): Expression = expr match {
    case a: Alias =>
      //logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarAlias(replaceWithColumnarExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case lit: Literal =>
      //logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarLiteral(lit)
    case binArith: BinaryArithmetic =>
      //logInfo(s"${expr.getClass} ${expr} is supported.")
      ColumnarBinaryArithmetic.create(replaceWithColumnarExpression(binArith.left), replaceWithColumnarExpression(binArith.right), expr)
    case b: BoundReference =>
      //logInfo(s"${expr.getClass} ${expr} is supported.")
      new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
    case expr =>
      logWarning(s"${expr.getClass} ${expr} is not currently supported.")
      expr
  }

}
