package com.intel.sparkColumnarPlugin.expression

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class ColumnarAlias(child: Expression, name: String)(
    override val exprId: ExprId,
    override val qualifier: Seq[String],
    override val explicitMetadata: Option[Metadata])
  extends Alias(child, name)(exprId, qualifier, explicitMetadata) {
  override def supportsColumnar(): Boolean = true

  override def columnarEval(input: Any): Any = child.columnarEval(input)
}

class ColumnarAttributeReference(att: AttributeReference) extends AttributeReference(att.name, att.dataType, att.nullable)(att.exprId, att.qualifier) {
  override def supportsColumnar(): Boolean = true
}

