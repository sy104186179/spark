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
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object ShufflePruning extends DynamicPruningBase with JoinSelectionHelper {

  private def isSupportBuildBloomFilter(exp: Expression): Boolean = {
    exp.dataType match {
      case BooleanType => true
      case ByteType | ShortType | IntegerType | LongType => true
      case FloatType | DoubleType => true
      case _: DecimalType => true
      case DateType | TimestampType => true
      case StringType | BinaryType => true
      case _ => false
    }
  }

  /**
   * Only pruning on supported type and non-partition columns and non-bucket columns.
   */
  private def canPruningExpr(exp: Expression, plan: LogicalPlan): Boolean = {
    isSupportBuildBloomFilter(exp) && findExpressionAndTrackLineageDown(exp, plan).exists {
      case (resExp, l @ LogicalRelation(fs: HadoopFsRelation, _, _, _)) =>
        val resolver = fs.sparkSession.sessionState.analyzer.resolver
        val partitionColumns = AttributeSet(l.resolve(fs.partitionSchema, resolver))
        val bucketColumns = fs.bucketSpec.map(_.bucketColumnNames)
          .map(_.flatMap(b => l.resolve(Seq(b), resolver))).map(AttributeSet(_))
        !resExp.references.exists(_.references.subsetOf(partitionColumns)) &&
          !bucketColumns.exists(b => resExp.references.exists(_.references.subsetOf(b)))
      case _ => false
    }
  }

  override def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKey: Expression,
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      hasBenefit2: Boolean): LogicalPlan = {

    val shuffleStages = pruningPlan.collect {
      case j @ Join(left, right, _, _, hint)
        if !canBroadcastBySize(left, SQLConf.get) && !canBroadcastBySize(right, SQLConf.get)
          && !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) => j
      case a: Aggregate => a
    }

    val broadcastHasBenefit = shuffleStages.flatMap(_.collectLeaves())
      .find(l => pruningKey.references.subsetOf(l.outputSet)) match {
      case Some(plan) =>
        plan.stats.sizeInBytes >= SQLConf.get.dynamicShufflePruningSideThreshold &&
          canBroadcastBySize(filteringPlan, SQLConf.get) &&
          canBroadcastBySize(filteringPlan.collectLeaves().maxBy(_.stats.sizeInBytes), SQLConf.get)
      case None => false
    }


    val filterRatio = SQLConf.get.getConfString("spark.sql.filterRatio", "1").toFloat
    val overhead = filteringPlan.collectLeaves().map(_.stats.sizeInBytes).sum.toFloat
    val a = filterRatio * pruningPlan.stats.sizeInBytes.toFloat
    val shuffleHasBenefit = a > overhead.toFloat
    // scalastyle:off
    println(s"filterRatio: ${filterRatio}, ${a}, ${overhead}")

    if (broadcastHasBenefit) {
      Filter(
        BloomFilterPruningSubquery(
          pruningKey,
          filteringPlan,
          joinKeys,
          joinKeys.indexOf(filteringKey)),
        pruningPlan)
    } else if (shuffleHasBenefit) {
      val namedExpressions = filteringKey.map { e =>
        new BuildBloomFilter(e).toAggregateExpression()
      }.map(e => Alias(e, e.toString)())
      val filterPlan = Aggregate(Nil, namedExpressions, RepartitionByExpression(joinKeys, filteringPlan, None))
      val index = joinKeys.indexOf(filteringKey)
      Filter(
        RuntimeBloomFilterPruningSubquery(pruningKey, filterPlan, joinKeys, index),
        pruningPlan)
    } else {
      pruningPlan
    }
  }

  /**
   * We assume the shuffle pruning has benefit if prunPlan has shuffle and it's leaf node larger
   * than `spark.sql.optimizer.dynamicShufflePruning.pruningSideThreshold` and otherPlan
   * can broadcast.
   */
  override def pruningHasBenefit(
      prunExpr: Expression,
      prunPlan: LogicalPlan,
      otherExpr: Expression,
      otherPlan: LogicalPlan): Boolean = {
    val shuffleStages = prunPlan.collect {
      case j @ Join(left, right, _, _, hint)
        if !canBroadcastBySize(left, SQLConf.get) && !canBroadcastBySize(right, SQLConf.get)
          && !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) => j
      case a: Aggregate => a
    }

    shuffleStages.flatMap(_.collectLeaves())
      .find(l => prunExpr.references.subsetOf(l.outputSet)) match {
      case Some(plan) =>
        plan.stats.sizeInBytes >= SQLConf.get.dynamicShufflePruningSideThreshold &&
          canBroadcastBySize(otherPlan, SQLConf.get) &&
          canBroadcastBySize(otherPlan.collectLeaves().maxBy(_.stats.sizeInBytes), SQLConf.get)
      case None => false
    }
  }

  override def prune(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      // skip this rule if there's already a RuntimeBloomFilter subquery on the LHS of a join.
      case j @ Join(Filter(_: BloomFilterSubqueryExpression, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: BloomFilterSubqueryExpression, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression) if fromDifferentSides(a, left, b, right) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            if (canPruneLeft(joinType) && canPruningExpr(l, left)) {
              val hasBenefit = pruningHasBenefit(l, newLeft, r, right)
              newLeft = insertPredicate(l, newLeft, r, right, rightKeys, hasBenefit)
            } else {
              if (canPruneRight(joinType) && canPruningExpr(r, right)) {
                val hasBenefit = pruningHasBenefit(r, newRight, l, left)
                newRight = insertPredicate(r, newRight, l, left, leftKeys, hasBenefit)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if SQLConf.get.dynamicShufflePruningEnabled => prune(plan)
    case _ => plan
  }
}
