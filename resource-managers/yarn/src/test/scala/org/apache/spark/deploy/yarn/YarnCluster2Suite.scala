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

package org.apache.spark.deploy.yarn

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher._
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart,
  SparkListenerExecutorAdded}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.tags.ExtendedYarnTest
import org.apache.spark.util.Utils

/**
 * Integration tests for YARN; these tests use a mini Yarn cluster to run Spark-on-YARN
 * applications, and require the Spark assembly to be built before they can be successfully
 * run.
 */
@ExtendedYarnTest
class YarnCluster2Suite extends BaseYarnClusterSuite {

  val cpuCores = 8
  val numNodeManagers = 10
  val coresTotal = cpuCores * numNodeManagers

  // override def newYarnConfig(): Configuration = new YarnConfiguration()

  override def newYarnConfig(): CapacitySchedulerConfiguration = {
    val queueNameRA = "ra"
    val queueNameRB = "rb"
    val queueNameA1 = "a1"
    val queueNameA2 = "a2"
    val ra = CapacitySchedulerConfiguration.ROOT + "." + queueNameRA
    val rb = CapacitySchedulerConfiguration.ROOT + "." + queueNameRB
    val a1 = ra + "." + queueNameA1
    val a2 = ra + "." + queueNameA2

    val aCapacity = 40F
    val aMaximumCapacity = 60F
    val bCapacity = 60F
    val bMaximumCapacity = 100F
    val a1Capacity = 30F
    val a1MaximumCapacity = 70F
    val a2Capacity = 70F

    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
    val yarnConf = new CapacitySchedulerConfiguration()

    // Define top-level queues
    yarnConf.setQueues(CapacitySchedulerConfiguration.ROOT, Array(queueNameRA, queueNameRB))
    yarnConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100)
    yarnConf.setCapacity(ra, aCapacity)
    yarnConf.setMaximumCapacity(ra, aMaximumCapacity)
    yarnConf.setCapacity(rb, bCapacity)
    yarnConf.setMaximumCapacity(rb, bMaximumCapacity)

    // Define 2nd-level queues
    yarnConf.setQueues(ra, Array(queueNameA1, queueNameA2))
    yarnConf.setCapacity(a1, a1Capacity)
    yarnConf.setMaximumCapacity(a1, a1MaximumCapacity)
    yarnConf.setCapacity(a2, a2Capacity)
    yarnConf.set("yarn.nodemanager.resource.cpu-vcores", cpuCores.toString)
    yarnConf
  }

  test("run Spark in yarn-cluster mode with using SparkHadoopUtil.conf2") {
    testYarnAppUseSparkHadoopUtilConf2()
  }

  private def testYarnAppUseSparkHadoopUtilConf2(): Unit = {
    val result = File.createTempFile("result", null, new java.io.File("/tmp/spark"))
    val finalState = runSpark(true,
      mainClassName(YarnClusterDriverUseSparkHadoopUtilConf2.getClass),
      appArgs = Seq("key=value", result.getAbsolutePath()),
      extraConf = Map("spark.hadoop.key" -> "value"))
    checkResult(finalState, result)
  }
}

private object YarnClusterDriverUseSparkHadoopUtilConf2 extends Logging with Matchers {
  def main(args: Array[String]): Unit = {

    var result = "failure"
    val status = new File(args(1))

    var sc: SparkContext = null
    try {
      sc = new SparkContext(new SparkConf()
         .set("spark.dynamicAllocation.enabled", "true")
          .set("spark.shuffle.service.enabled", "true")
          .set(QUEUE_NAME, "a1")
        .setAppName("yarn test using SparkHadoopUtil's conf"))

      sc.parallelize(1 to 10).count()
      assert(sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS) === Int.MaxValue)
      result = "success"
    } catch {
      case ex: Exception => result = ex.getMessage
        Files.write(result, status, StandardCharsets.UTF_8)
    } finally {
      Files.write(result, status, StandardCharsets.UTF_8)
      sc.stop()
    }
  }
}

