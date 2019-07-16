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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark

/**
 * To run this benchmark:
 *   build/sbt "sql/test:runMain org.apache.spark.sql.execution.benchmark.SPARK_28348_Benchmark"
 */
object SPARK_28348_Benchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val title = "Benchmark SPARK-28348"
    runBenchmark(title) {
      withTempPath { dir =>
        val N = 6000000
        val df = spark.range(N)

        df.selectExpr(
          "cast(id as decimal(38, 18)) as id1",
          "cast(id % 999999 as decimal(38, 18)) as id2")
          .write.mode("overwrite").parquet(dir.getCanonicalPath)

        val benchmark = new Benchmark(title, N, minNumIters = 5, output = output)
        Seq(false, true).foreach { customCheckOverflow =>
          val name = if (customCheckOverflow) {
            "Enable Custom CheckOverflow"
          } else {
            "Disable Custom CheckOverflow"
          }
          benchmark.addCase(name) { _ =>
            withSQLConf("spark.sql.customCheckOverflow" -> customCheckOverflow.toString) {
              spark.read.parquet(dir.getCanonicalPath)
                .selectExpr("cast(id1 * id2 as decimal(38, 18))").collect()
            }
          }
        }
        benchmark.run()
      }
    }
  }
}
