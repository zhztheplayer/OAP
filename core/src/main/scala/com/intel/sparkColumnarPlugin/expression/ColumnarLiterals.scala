package com.intel.sparkColumnarPlugin.expression

import org.apache.spark.sql.catalyst.expressions._

class ColumnarLiteral(lit: Literal) extends Literal(lit.value, lit.dataType) {
  override def supportsColumnar(): Boolean = true

  override def columnarEval(input: Any): Any = lit.value
}

