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

import scala.collection.JavaConverters._
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
class DynamicSetMaxExecutorsSuite extends BaseYarnClusterSuite {

  val cpuCores = 8
  val numNodeManagers = 10
  val coresTotal = cpuCores * numNodeManagers

  override def newYarnConfig(): CapacitySchedulerConfiguration = {
    val ra = CapacitySchedulerConfiguration.ROOT + ".ra"
    val rb = CapacitySchedulerConfiguration.ROOT + ".rb"
    val a1 = ra + ".a1"
    val a2 = ra + ".a2"

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
    yarnConf.setQueues(CapacitySchedulerConfiguration.ROOT, Array("ra", "rb"))
    yarnConf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100)
    yarnConf.setCapacity(ra, aCapacity)
    yarnConf.setMaximumCapacity(ra, aMaximumCapacity)
    yarnConf.setCapacity(rb, bCapacity)
    yarnConf.setMaximumCapacity(rb, bMaximumCapacity)

    // Define 2nd-level queues
    yarnConf.setQueues(ra, Array("a1", "a2"))
    yarnConf.setCapacity(a1, a1Capacity)
    yarnConf.setMaximumCapacity(a1, a1MaximumCapacity)
    yarnConf.setCapacity(a2, a2Capacity)
    yarnConf.set("yarn.nodemanager.resource.cpu-vcores", cpuCores.toString)
    yarnConf.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
      "100.0")
    yarnConf
  }

  override def runSpark(
                          clientMode: Boolean,
                          klass: String,
                          appArgs: Seq[String] = Nil,
                          sparkArgs: Seq[(String, String)] = Nil,
                          extraClassPath: Seq[String] = Nil,
                          extraJars: Seq[String] = Nil,
                          extraConf: Map[String, String] = Map(),
                          extraEnv: Map[String, String] = Map()): SparkAppHandle.State = {
    val deployMode = if (clientMode) "client" else "cluster"
    val propsFile = createConfFile(extraClassPath = extraClassPath, extraConf = extraConf)
    println(s"hadoopConfDir.getAbsolutePath: ${hadoopConfDir.getAbsolutePath}")
    val env = Map("YARN_CONF_DIR" -> hadoopConfDir.getAbsolutePath()) ++ extraEnv

    val sparkConf = new SparkConf().
      setAppName(getClass.getName).
      setSparkHome(sys.props("spark.test.home")).
      setMaster("yarn").
      set("spark.dynamicAllocation.enabled", "true").
      set(QUEUE_NAME, "a")

    val sc = new SparkContext(sparkConf)

    sc.conf.get(DYN_ALLOCATION_MAX_EXECUTORS)

    println(s"########DYN_ALLOCATION_MAX_EXECUTORS: ${sc.conf.get(DYN_ALLOCATION_MAX_EXECUTORS)}")

    sc.parallelize(1 to 1000).repartition(100).count()

    println(s"########DYN_ALLOCATION_MAX_EXECUTORS: ${sc.conf.get(DYN_ALLOCATION_MAX_EXECUTORS)}")


    SparkAppHandle.State.FAILED
  }

  test("run Spark in yarn-client mode") {
    testBasicYarnApp(true)
  }

  test("run Spark in yarn-cluster mode") {
    testBasicYarnApp(false)
  }

  private def testBasicYarnApp(clientMode: Boolean, conf: Map[String, String] = Map()): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClusterDriver.getClass),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = conf)
    checkResult(finalState, result)
  }


}
