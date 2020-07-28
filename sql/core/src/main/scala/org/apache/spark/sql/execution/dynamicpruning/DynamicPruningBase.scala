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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A base class for DynamicPruning.
 */
abstract class DynamicPruningBase extends Rule[LogicalPlan] with PredicateHelper {

  protected def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKey: Expression,
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      hasBenefit: Boolean): LogicalPlan

  protected def pruningHasBenefit(
      partExpr: Expression,
      partPlan: LogicalPlan,
      otherExpr: Expression,
      otherPlan: LogicalPlan): Boolean

  protected def prune(plan: LogicalPlan): LogicalPlan

  protected def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  protected def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter => true
    case _ => false
  }

  // checks if two expressions are on opposite sides of the join
  protected def fromDifferentSides(
      x: Expression,
      left: LogicalPlan,
      y: Expression,
      right: LogicalPlan): Boolean = {
    def fromLeftRight(x: Expression, y: Expression) =
      !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
        !y.references.isEmpty && y.references.subsetOf(right.outputSet)
    fromLeftRight(x, y) || fromLeftRight(y, x)
  }
}
