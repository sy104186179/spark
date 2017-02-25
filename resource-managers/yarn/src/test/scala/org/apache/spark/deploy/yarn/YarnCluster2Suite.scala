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
    yarnConf
  }

  test("run Spark in yarn-cluster mode with using SparkHadoopUtil.conf2") {
    testYarnAppUseSparkHadoopUtilConf2()
  }

//  test("run Spark in yarn-client mode with different configurations") {
//    testBasicYarnApp(true,
//      Map(
//        "spark.driver.memory" -> "512m",
//        "spark.executor.cores" -> "1",
//        "spark.executor.memory" -> "512m",
//        "spark.executor.instances" -> "2"
//      ))
//  }

//  test("monitor app using launcher library2") {
//    val env = new JHashMap[String, String]()
//    env.put("YARN_CONF_DIR", hadoopConfDir.getAbsolutePath())
//
//    val propsFile = createConfFile()
//    val handle = new SparkLauncher(env)
//      .setSparkHome(sys.props("spark.test.home"))
//      .setConf("spark.ui.enabled", "false")
//      .setPropertiesFile(propsFile)
//      .setMaster("yarn")
//      .setDeployMode("client")
//      .setAppResource(SparkLauncher.NO_RESOURCE)
//      .setMainClass(mainClassName(YarnLauncherTestApp.getClass))
//      .startApplication()
//
//    try {
//      eventually(timeout(30 seconds), interval(100 millis)) {
//        handle.getState() should be (SparkAppHandle.State.RUNNING)
//      }
//
//      handle.getAppId() should not be (null)
//      handle.getAppId() should startWith ("application_")
//      handle.stop()
//
//      eventually(timeout(30 seconds), interval(100 millis)) {
//        handle.getState() should be (SparkAppHandle.State.KILLED)
//      }
//    } finally {
//      handle.kill()
//    }
//  }

  private def testYarnAppUseSparkHadoopUtilConf2(): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(false,
      mainClassName(YarnClusterDriverUseSparkHadoopUtilConf2.getClass),
      appArgs = Seq("key=value", result.getAbsolutePath()),
      extraConf = Map("spark.hadoop.key" -> "value"))
    checkResult(finalState, result)
  }

  private def testBasicYarnApp(clientMode: Boolean, conf: Map[String, String] = Map()): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClusterDriver2.getClass),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = conf)
    checkResult(finalState, result)
  }

}

private[spark] class SaveExecutorInfo2 extends SparkListener {
  val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()
  var driverLogs: Option[collection.Map[String, String]] = None

  override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
    addedExecutorInfos(executor.executorId) = executor.executorInfo
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart): Unit = {
    driverLogs = appStart.driverLogs
  }
}

private object YarnClusterDriverUseSparkHadoopUtilConf2 extends Logging with Matchers {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      // scalastyle:off println
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClusterDriverUseSparkHadoopUtilConf [hadoopConfKey=value] [result file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf()
      .set("spark.extraListeners", classOf[SaveExecutorInfo].getName)
      .setAppName("yarn test using SparkHadoopUtil's conf"))
    logWarning(s"1sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS)" +
      s": ${sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS)}")
    sc.parallelize(1 to 1000, 200).count()
    logWarning(s"2sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS)" +
      s": ${sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS)}")
    // assert(sc.getConf.get(DYN_ALLOCATION_MAX_EXECUTORS) === Int.MaxValue)
    val kv = args(0).split("=")
    val status = new File(args(1))
    var result = "failure"
    try {
      SparkHadoopUtil.get.conf.get(kv(0)) should be (kv(1))
      result = "success"
    } finally {
      Files.write(result, status, StandardCharsets.UTF_8)
      sc.stop()
    }
  }
}

private object YarnClusterDriver2 extends Logging with Matchers {

  val WAIT_TIMEOUT_MILLIS = 10000

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      // scalastyle:off println
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClusterDriver [result file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }

    val conf = new SparkConf()
      .set("spark.extraListeners", classOf[SaveExecutorInfo2].getName)
      .set("spark.dynamicAllocation.enabled", "true")
      .setAppName("yarn \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns")
    val sc = new SparkContext(conf)
    val fixedConf = sc.getConf

    println(s"DYN_ALLOCATION_MAX_EXECUTORS: ${fixedConf.get(DYN_ALLOCATION_MAX_EXECUTORS)}")

    val status = new File(args(0))
    var result = "failure"
    try {
      val data = sc.parallelize(1 to 4, 4).collect().toSet
      sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      data should be (Set(1, 2, 3, 4))
      result = "success"

      // Verify that the config archive is correctly placed in the classpath of all containers.
      val confFile = "/" + Client.SPARK_CONF_FILE
      assert(getClass().getResource(confFile) != null)
      val configFromExecutors = sc.parallelize(1 to 4, 4)
        .map { _ => Option(getClass().getResource(confFile)).map(_.toString).orNull }
        .collect()
      assert(configFromExecutors.find(_ == null) === None)
    } finally {
      Files.write(result, status, StandardCharsets.UTF_8)
      sc.stop()
    }

    // verify log urls are present
    val listeners = sc.listenerBus.findListenersByClass[SaveExecutorInfo]
    assert(listeners.size === 1)
    val listener = listeners(0)
    val executorInfos = listener.addedExecutorInfos.values
    assert(executorInfos.nonEmpty)
    executorInfos.foreach { info =>
      assert(info.logUrlMap.nonEmpty)
    }

    // If we are running in yarn-cluster mode, verify that driver logs links and present and are
    // in the expected format.
    if (conf.get("spark.submit.deployMode") == "cluster") {
      assert(listener.driverLogs.nonEmpty)
      val driverLogs = listener.driverLogs.get
      assert(driverLogs.size === 2)
      assert(driverLogs.contains("stderr"))
      assert(driverLogs.contains("stdout"))
      val urlStr = driverLogs("stderr")
      // Ensure that this is a valid URL, else this will throw an exception
      new URL(urlStr)
      val containerId = YarnSparkHadoopUtil.get.getContainerId
      val user = Utils.getCurrentUserName()
      assert(urlStr.endsWith(s"/node/containerlogs/$containerId/$user/stderr?start=-4096"))
    }
  }

}
