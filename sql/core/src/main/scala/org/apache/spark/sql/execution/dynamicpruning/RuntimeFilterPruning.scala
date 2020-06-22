/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * Dynamic partition pruning optimization is performed based on the type and
 * selectivity of the join operation. During query optimization, we insert a
 * predicate on the partitioned table using the filter from the other side of
 * the join and a custom wrapper called DynamicPruning.
 *
 * The basic mechanism for DPP inserts a duplicated subquery with the filter from the other side,
 * when the following conditions are met:
 *    (1) the table to prune is partitioned by the JOIN key
 *    (2) the join operation is one of the following types: INNER, LEFT SEMI (partitioned on left),
 *    LEFT OUTER (partitioned on right), or RIGHT OUTER (partitioned on left)
 *
 * In order to enable partition pruning directly in broadcasts, we use a custom DynamicPruning
 * clause that incorporates the In clause with the subquery and the benefit estimation.
 * During query planning, when the join type is known, we use the following mechanism:
 *    (1) if the join is a broadcast hash join, we replace the duplicated subquery with the reused
 *    results of the broadcast,
 *    (2) else if the estimated benefit of partition pruning outweighs the overhead of running the
 *    subquery query twice, we keep the duplicated subquery
 *    (3) otherwise, we drop the subquery.
 */
object RuntimeFilterPruning extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Insert a dynamic partition pruning predicate on one side of the join using the filter on the
   * other side of the join.
   *  - to be able to identify this filter during query planning, we use a custom
   *    DynamicPruning expression that wraps a regular In expression
   *  - we also insert a flag that indicates if the subquery duplication is worthwhile and it
   *  should run regardless of the join strategy, or is too expensive and it should be run only if
   *  we can reuse the results of a broadcast
   */
  private def insertPredicate(
      pruningKey: Seq[Expression],
      pruningPlan: LogicalPlan,
      filteringKey: Seq[Expression],
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      hasBenefit: Boolean): LogicalPlan = {
    if (hasBenefit) {
      val namedExpressions = filteringKey.flatMap { e =>
        Seq(Min(e).toAggregateExpression(), Max(e).toAggregateExpression())
      }.map(e => Alias(e, e.toString)())
      val filterPlan = Aggregate(Nil, namedExpressions, filteringPlan)
      // insert a DynamicPruning wrapper to identify the subquery during query planning
      Filter(RuntimeMinMaxPruningSubquery(pruningKey, filterPlan, joinKeys), pruningPlan)
    } else {
      // abort dynamic partition pruning
      pruningPlan
    }
  }

  /**
   * Given an estimated filtering ratio we assume the partition pruning has benefit if
   * the size in bytes of the partitioned plan after filtering is greater than the size
   * in bytes of the plan on the other side of the join. We estimate the filtering ratio
   * using column statistics if they are available, otherwise we use the config value of
   * `spark.sql.optimizer.joinFilterRatio`.
   */
  private def pruningHasBenefit(
      partExpr: Seq[Expression],
      partPlan: LogicalPlan,
      otherExpr: Seq[Expression],
      otherPlan: LogicalPlan): Boolean = {

    // get the distinct counts of an attribute for a given table
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }

    // the default filtering ratio when CBO stats are missing, but there is a
    // predicate that is likely to be selective
    val fallbackRatio = SQLConf.get.dynamicFilterPruningFallbackFilterRatio
    // the filtering ratio based on the type of the join condition and on the column statistics
    val filterRatio = (partExpr.reduceLeft(And).references.toList,
        otherExpr.reduceLeft(And).references.toList) match {
      // filter out expressions with more than one attribute on any side of the operator
      case (leftAttr :: Nil, rightAttr :: Nil)
        if SQLConf.get.dynamicFilterPruningUseStats =>
          // get the CBO stats for each attribute in the join condition
          val partDistinctCount = distinctCounts(leftAttr, partPlan)
          val otherDistinctCount = distinctCounts(rightAttr, otherPlan)
          val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
            otherDistinctCount.isDefined
          if (!availableStats) {
            fallbackRatio
          } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
            // there is likely an estimation error, so we fallback
            fallbackRatio
          } else {
            1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble
          }
      case _ => fallbackRatio
    }

    // the pruning overhead is the total size in bytes of all scan relations
    val overhead = otherPlan.collectLeaves().map(_.stats.sizeInBytes).sum.toFloat

    filterRatio * partPlan.stats.sizeInBytes.toFloat > overhead.toFloat
    // scalastyle:off
    println(s"filterRatio: ${filterRatio}")
    println(s"overhead: ${overhead}")
    println(s"partPlan.stats.sizeInBytes.toFloat: ${partPlan.stats.sizeInBytes.toFloat}")
    println(s"partPlan.stats.sizeInBytes.toDouble: ${partPlan.stats.sizeInBytes.toDouble}")
    println(s"partPlan.stats.sizeInBytes: ${partPlan.stats.sizeInBytes}")
    println(s"filterRatio * partPlan.stats.sizeInBytes.toFloat: ${filterRatio * partPlan.stats.sizeInBytes.toFloat}")
    println(s"overhead.toFloat: ${overhead.toFloat}")
    true
  }

  /**
   * Returns whether an expression is likely to be selective
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case Like(_, _, _) => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _ => false
  }

  /**
   * Search a filtering predicate in a given logical plan
   */
  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }.isDefined
  }

  private def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  private def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter => true
    case _ => false
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // skip this rule if there's already a DPP subquery on the LHS of a join
      case j @ Join(Filter(_: RuntimeMinMaxPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: RuntimeMinMaxPruningSubquery, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        // checks if two expressions are on opposite sides of the join
        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)
          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        val (leftList, rightList) = splitConjunctivePredicates(condition).flatMap {
          case EqualTo(a: Expression, b: Expression) if fromDifferentSides(a, b) => Seq(a, b)
          case _ => Nil
        }.partition(_.references.subsetOf(left.outputSet))

        // there should be a partitioned table and a filter on the dimension table,
        // otherwise the pruning will not trigger
        if (canPruneLeft(joinType)) {
          val hasBenefit = pruningHasBenefit(leftList, left, rightList, right)
          newLeft = insertPredicate(leftList, newLeft, rightList, right, rightKeys, hasBenefit)
        } else {
          if (canPruneRight(joinType)) {
            val hasBenefit = pruningHasBenefit(rightList, right, leftList, left)
            newRight = insertPredicate(rightList, newRight, leftList, left, leftKeys, hasBenefit)
          }
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if !SQLConf.get.dynamicFilterPruningEnabled => plan
    case _ => prune(plan)
  }
}
