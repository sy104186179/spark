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
import org.apache.spark.sql.catalyst.expressions.aggregate.BuildBloomFilter
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.dynamicpruning.PartitionPruning.findExpressionAndTrackLineageDown
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

  def filterPartitionColumn(col: Expression, plan: LogicalPlan): Boolean = {
    findExpressionAndTrackLineageDown(col, plan).exists {
      case (resExp, l@LogicalRelation(fs: HadoopFsRelation, _, _, _)) =>
        val partitionColumns = AttributeSet(
          l.resolve(fs.partitionSchema, fs.sparkSession.sessionState.analyzer.resolver))
        resExp.references.subsetOf(partitionColumns)
      case _ => false
    }
  }

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
      val namedExpressions = filteringKey.map { e =>
        new BuildBloomFilter(e).toAggregateExpression()
      }.map(e => Alias(e, e.toString)())
      val filterPlan = Aggregate(Nil, namedExpressions, filteringPlan)
      // insert a DynamicPruning wrapper to identify the subquery during query planning
      Filter(RuntimeBloomFilterPruningSubquery(pruningKey, filterPlan, joinKeys), pruningPlan)
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

    // scalastyle:off
    println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    println(s"partExpr: ${partExpr.mkString(".")}")
    println(s"partPlan.stats.sizeInBytes: ${partPlan.stats.sizeInBytes}")
    println(s"partPlan.collectLeaves().map(_.stats.sizeInBytes).sum: ${partPlan.collectLeaves().map(_.stats.sizeInBytes).sum}")
    println(s"partPlan.collectLeaves().map(_.stats.sizeInBytes).max: ${partPlan.collectLeaves().map(_.stats.sizeInBytes).max}")

    println(s"otherExpr: ${otherExpr.mkString(",")}")
    println(s"otherPlan.stats.sizeInBytes: ${otherPlan.stats.sizeInBytes}")
    println(s"otherPlan.collectLeaves().map(_.stats.sizeInBytes).sum: ${otherPlan.collectLeaves().map(_.stats.sizeInBytes).sum}")
    println(s"otherPlan.collectLeaves().map(_.stats.sizeInBytes).max: ${otherPlan.collectLeaves().map(_.stats.sizeInBytes).max}")

    val optimized3 = partPlan.collectLeaves().map(_.stats.sizeInBytes).sum / otherPlan.stats.sizeInBytes
    val ret = optimized3 > SQLConf.get.dynamicFilterPruningSmallerRatio
    println(s"optimized3: ${optimized3}, ${ret}")
    println("#######################################")
    ret
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
      case j @ Join(Filter(_: RuntimeBloomFilterPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: RuntimeBloomFilterPruningSubquery, _), _, _, _) => j
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

        val nonLeftListCols = leftList.filterNot(c => filterPartitionColumn(c, left))
        val nonRightListCols = rightList.filterNot(c => filterPartitionColumn(c, right))
        // there should be a partitioned table and a filter on the dimension table,
        // otherwise the pruning will not trigger
        if (canPruneLeft(joinType) && nonLeftListCols.nonEmpty) {
          val hasBenefit = pruningHasBenefit(nonLeftListCols, left, rightList, right)
          newLeft = insertPredicate(nonLeftListCols, newLeft, rightList, right, rightKeys, hasBenefit)
        } else {
          if (canPruneRight(joinType) && nonRightListCols.nonEmpty) {
            val hasBenefit = pruningHasBenefit(nonRightListCols, right, leftList, left)
            newRight = insertPredicate(nonRightListCols, newRight, leftList, left, leftKeys, hasBenefit)
          }
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  // TODO:
  //   1. BuildBloomFilter and InBloomFilter support codegen.
  //   2. Split Filter to DynamicFilter and Filter, and DynamicFilter should support filter pushdown.
  //   3. BroadcastExchange reuse.
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if SQLConf.get.dynamicFilterPruningEnabled => prune(plan)
    case _ => plan
  }
}
