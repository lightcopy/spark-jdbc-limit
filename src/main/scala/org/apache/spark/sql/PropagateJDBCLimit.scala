package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LocalLimit}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc._

/**
 * Rule to push down limit to JDBC relation.
 * This works for both `df.show()` or `df.limit(X)`.
 * Simply add this rule to spark extra optimizations sequence.
 */
object PropagateJDBCLimit extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case limit @ LocalLimit(
      limitValue @ IntegerLiteral(value),
      rel @ LogicalRelation(
        prev @ JDBCRelation(parts, jdbcOptions), _, table)) =>
      // this is done to preserve aliases for expressions
      val attr = rel.attributeMap.values.toList
      val updatedRel = LogicalRelation(
        JDBCRelationWithLimit(parts, jdbcOptions, value)(prev.sparkSession),
        attr,
        table)
      LocalLimit(limitValue, updatedRel)
  }
}
