package com.intel.sparkcolumnarPlugin

import com.intel.sparkcolumnarPlugin.expression._
import com.intel.sparkcolumnarPlugin.execution._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

case class ColumnarOverrides() extends Rule[SparkPlan] {
  def replaceWithColumnarExpression(exp: Expression): Expression = exp match {
    case a: Alias =>
      logInfo(s"Columnar Processing for expression ${exp.getClass} ${exp} is supported.")
      //Alias(replaceWithColumnarExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      new ColumnarAlias(replaceWithColumnarExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case att: AttributeReference =>
      logInfo(s"Columnar Processing for expression ${exp.getClass} ${exp} is supported.")
      new ColumnarAttributeReference(att)
    case lit: Literal =>
      logInfo(s"Columnar Processing for expression ${exp.getClass} ${exp} is supported.")
      new ColumnarLiteral(lit)
      //lit // No sub expressions and already supports columnar so just return it
    case add: Add =>
      logInfo(s"Columnar Processing for expression ${exp.getClass} ${exp} is supported.")
      new ColumnarAdd(replaceWithColumnarExpression(add.left), replaceWithColumnarExpression(add.right))
    case mul: Multiply =>
      logInfo(s"Columnar Processing for expression ${exp.getClass} ${exp} is supported.")
      new ColumnarMultiply(replaceWithColumnarExpression(mul.left), replaceWithColumnarExpression(mul.right))
    case exp =>
      logWarning(s"Columnar Processing for expression ${exp.getClass} ${exp} is not currently supported.")
      exp
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: ProjectExec =>
      logWarning(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarProjectExec(plan.projectList.map((exp) =>
        replaceWithColumnarExpression(exp).asInstanceOf[NamedExpression]),
        replaceWithColumnarPlan(plan.child))
    case plan: BatchScanExec =>
      logWarning(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarBatchScanExec(plan.output.map((exp) =>
        replaceWithColumnarExpression(exp).asInstanceOf[AttributeReference]),
        plan.scan)
    case p =>
      logWarning(s"Columnar Processing for ${p.getClass} is not currently supported.")
      p.withNewChildren(p.children.map(replaceWithColumnarPlan))
  }

  def apply(plan: SparkPlan) :SparkPlan = {
    replaceWithColumnarPlan(plan)
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def columnarEnabled = session.sqlContext.
    getConf("org.apache.spark.example.columnar.enabled", "true").trim.toBoolean
  val overrides = ColumnarOverrides()

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      overrides(plan)
    } else {
      plan
    }
  }
}

/**
  * Extension point to enable columnar processing.
  *
  * To run with columnar set spark.sql.extensions to org.apache.spark.example.Plugin
  */
class ColumnarPlugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing extensions to enable columnar CPU support." +
      " To disable this set `org.apache.spark.example.columnar.enabled` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
  }
}
