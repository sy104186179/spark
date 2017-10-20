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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf

class StringCompareCaseInsensitiveSuite extends PlanTest with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
    originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
  }

  private def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(actual, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  private var sparkSession: SparkSession = _

  private def withCaseInsensitive[T](caseInsensitive: String)(f: SparkSession => T): T = {
    try {
      val sparkConf = new SparkConf(false)
        .setMaster("local")
        .setAppName(this.getClass.getName)
        .set("spark.ui.enabled", "false")
        .set("spark.driver.allowMultipleContexts", "true")
        .set(SQLConf.STRING_COMPARE_CASE_INSENSITIVE.key, caseInsensitive)

      sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      f(sparkSession)
    } finally {
      if (sparkSession != null) {
        sparkSession.stop()
      }
    }
  }

  test("String compare case insensitive") {
    val sql = "select 'a' = 'A', 'a' > 'A', 'a' < 'A'"
    withCaseInsensitive("true") { spark =>
      checkAnswer(spark.sql(sql), Row(true, false, false) :: Nil)
    }

    withCaseInsensitive("false") { spark =>
      checkAnswer(spark.sql(sql), Row(false, true, false) ::  Nil)
    }
  }
}
