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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.SharedSparkSession

class ZorderSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  test("basic zorder sorting") {
    val input = Seq(
      (0, 0),
      (0, 1),
      (0, 2),
      (0, 3),
      (1, 0),
      (1, 1),
      (1, 2),
      (1, 3),
      (2, 0),
      (2, 1),
      (3, 0),
      (2, 2),
      (2, 3),
      (3, 1),
      (3, 2),
      (3, 3)
    )
    checkAnswer(
      input.toDF("a", "b"),
      (child: SparkPlan) => ZorderExec(Seq('a, 'b), global = true, child = child),
      input.map(Row.fromTuple),
      sortAnswers = false)
  }
}
