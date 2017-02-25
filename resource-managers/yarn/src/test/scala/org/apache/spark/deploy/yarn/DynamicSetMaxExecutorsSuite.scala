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

  val ra = CapacitySchedulerConfiguration.ROOT + ".a"
  val rb = CapacitySchedulerConfiguration.ROOT + ".b"
  val a1 = ra + ".a1"
  val a2 = ra + ".a2"

  val aCapacity = 40F
  val aMaximumCapacity = 60F
  val bCapacity = 60F
  val bMaximumCapacity = 100F
  val a1Capacity = 30F
  val a1MaximumCapacity = 70F
  val a2Capacity = 70F

  val cpuCores = 8
  val numNodeManagers = 10
  val coresTotal = cpuCores * numNodeManagers

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

  override def newYarnConfig(): Configuration = yarnConf

  private val TEST_PYFILE = """
    |import mod1, mod2
    |import sys
    |from operator import add
    |
    |from pyspark import SparkConf , SparkContext
    |if __name__ == "__main__":
    |    if len(sys.argv) != 2:
    |        print >> sys.stderr, "Usage: test.py [result file]"
    |        exit(-1)
    |    sc = SparkContext(conf=SparkConf())
    |    status = open(sys.argv[1],'w')
    |    result = "failure"
    |    rdd = sc.parallelize(range(10)).map(lambda x: x * mod1.func() * mod2.func())
    |    cnt = rdd.count()
    |    if cnt == 10:
    |        result = "success"
    |    status.write(result)
    |    status.close()
    |    sc.stop()
    """.stripMargin

  private val TEST_PYMODULE = """
    |def func():
    |    return 42
    """.stripMargin

  test("run Spark in yarn-client mode") {
    testBasicYarnApp(true)
  }

  test("run Spark in yarn-cluster mode") {
    testBasicYarnApp(false)
  }

  test("run Spark in yarn-client mode with different configurations") {
    testBasicYarnApp(true,
      Map(
        "spark.driver.memory" -> "512m",
        "spark.executor.cores" -> "1",
        "spark.executor.memory" -> "512m",
        "spark.executor.instances" -> "2"
      ))
  }

  test("run Spark in yarn-cluster mode with different configurations") {
    testBasicYarnApp(false,
      Map(
        "spark.driver.memory" -> "512m",
        "spark.driver.cores" -> "1",
        "spark.executor.cores" -> "1",
        "spark.executor.memory" -> "512m",
        "spark.executor.instances" -> "2"
      ))
  }

  test("run Spark in yarn-cluster mode with using SparkHadoopUtil.conf") {
    testYarnAppUseSparkHadoopUtilConf()
  }

  test("run Spark in yarn-client mode with additional jar") {
    testWithAddJar(true)
  }

  test("run Spark in yarn-cluster mode with additional jar") {
    testWithAddJar(false)
  }

  test("run Spark in yarn-cluster mode unsuccessfully") {
    // Don't provide arguments so the driver will fail.
    val finalState = runSpark(false, mainClassName(YarnClusterDriver.getClass))
    finalState should be (SparkAppHandle.State.FAILED)
  }

  test("run Spark in yarn-cluster mode failure after sc initialized") {
    val finalState = runSpark(false, mainClassName(YarnClusterDriverWithFailure.getClass))
    finalState should be (SparkAppHandle.State.FAILED)
  }

  test("run Python application in yarn-client mode") {
    testPySpark(true)
  }

  test("run Python application in yarn-cluster mode") {
    testPySpark(false)
  }

  test("run Python application in yarn-cluster mode using " +
    " spark.yarn.appMasterEnv to override local envvar") {
    testPySpark(
      clientMode = false,
      extraConf = Map(
        "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON"
          -> sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python"),
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON"
          -> sys.env.getOrElse("PYSPARK_PYTHON", "python")),
      extraEnv = Map(
        "PYSPARK_DRIVER_PYTHON" -> "not python",
        "PYSPARK_PYTHON" -> "not python"))
  }

  test("user class path first in client mode") {
    testUseClassPathFirst(true)
  }

  test("user class path first in cluster mode") {
    testUseClassPathFirst(false)
  }

  test("monitor app using launcher library") {
    val env = new JHashMap[String, String]()
    env.put("YARN_CONF_DIR", hadoopConfDir.getAbsolutePath())

    val propsFile = createConfFile()
    val handle = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf("spark.ui.enabled", "false")
      .setPropertiesFile(propsFile)
      .setMaster("yarn")
      .setDeployMode("client")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(mainClassName(YarnLauncherTestApp.getClass))
      .startApplication()

    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.RUNNING)
      }

      handle.getAppId() should not be (null)
      handle.getAppId() should startWith ("application_")
      handle.stop()

      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }

  test("timeout to get SparkContext in cluster mode triggers failure") {
    val timeout = 2000
    val finalState = runSpark(false, mainClassName(SparkContextTimeoutApp.getClass),
      appArgs = Seq((timeout * 4).toString),
      extraConf = Map(AM_MAX_WAIT_TIME.key -> timeout.toString))
    finalState should be (SparkAppHandle.State.FAILED)
  }

  private def testBasicYarnApp(clientMode: Boolean, conf: Map[String, String] = Map()): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClusterDriver.getClass),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = conf)
    checkResult(finalState, result)
  }

  private def testYarnAppUseSparkHadoopUtilConf(): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(false,
      mainClassName(YarnClusterDriverUseSparkHadoopUtilConf.getClass),
      appArgs = Seq("key=value", result.getAbsolutePath()),
      extraConf = Map("spark.hadoop.key" -> "value"))
    checkResult(finalState, result)
  }

  private def testWithAddJar(clientMode: Boolean): Unit = {
    val originalJar = TestUtils.createJarWithFiles(Map("test.resource" -> "ORIGINAL"), tempDir)
    val driverResult = File.createTempFile("driver", null, tempDir)
    val executorResult = File.createTempFile("executor", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClasspathTest.getClass),
      appArgs = Seq(driverResult.getAbsolutePath(), executorResult.getAbsolutePath()),
      extraClassPath = Seq(originalJar.getPath()),
      extraJars = Seq("local:" + originalJar.getPath()))
    checkResult(finalState, driverResult, "ORIGINAL")
    checkResult(finalState, executorResult, "ORIGINAL")
  }

  private def testPySpark(
      clientMode: Boolean,
      extraConf: Map[String, String] = Map(),
      extraEnv: Map[String, String] = Map()): Unit = {
    val primaryPyFile = new File(tempDir, "test.py")
    Files.write(TEST_PYFILE, primaryPyFile, StandardCharsets.UTF_8)

    // When running tests, let's not assume the user has built the assembly module, which also
    // creates the pyspark archive. Instead, let's use PYSPARK_ARCHIVES_PATH to point at the
    // needed locations.
    val sparkHome = sys.props("spark.test.home")
    val pythonPath = Seq(
        s"$sparkHome/python/lib/py4j-0.10.4-src.zip",
        s"$sparkHome/python")
    val extraEnvVars = Map(
      "PYSPARK_ARCHIVES_PATH" -> pythonPath.map("local:" + _).mkString(File.pathSeparator),
      "PYTHONPATH" -> pythonPath.mkString(File.pathSeparator)) ++ extraEnv

    val moduleDir =
      if (clientMode) {
        // In client-mode, .py files added with --py-files are not visible in the driver.
        // This is something that the launcher library would have to handle.
        tempDir
      } else {
        val subdir = new File(tempDir, "pyModules")
        subdir.mkdir()
        subdir
      }
    val pyModule = new File(moduleDir, "mod1.py")
    Files.write(TEST_PYMODULE, pyModule, StandardCharsets.UTF_8)

    val mod2Archive = TestUtils.createJarWithFiles(Map("mod2.py" -> TEST_PYMODULE), moduleDir)
    val pyFiles = Seq(pyModule.getAbsolutePath(), mod2Archive.getPath()).mkString(",")
    val result = File.createTempFile("result", null, tempDir)

    val finalState = runSpark(clientMode, primaryPyFile.getAbsolutePath(),
      sparkArgs = Seq("--py-files" -> pyFiles),
      appArgs = Seq(result.getAbsolutePath()),
      extraEnv = extraEnvVars,
      extraConf = extraConf)
    checkResult(finalState, result)
  }

  private def testUseClassPathFirst(clientMode: Boolean): Unit = {
    // Create a jar file that contains a different version of "test.resource".
    val originalJar = TestUtils.createJarWithFiles(Map("test.resource" -> "ORIGINAL"), tempDir)
    val userJar = TestUtils.createJarWithFiles(Map("test.resource" -> "OVERRIDDEN"), tempDir)
    val driverResult = File.createTempFile("driver", null, tempDir)
    val executorResult = File.createTempFile("executor", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClasspathTest.getClass),
      appArgs = Seq(driverResult.getAbsolutePath(), executorResult.getAbsolutePath()),
      extraClassPath = Seq(originalJar.getPath()),
      extraJars = Seq("local:" + userJar.getPath()),
      extraConf = Map(
        "spark.driver.userClassPathFirst" -> "true",
        "spark.executor.userClassPathFirst" -> "true"))
    checkResult(finalState, driverResult, "OVERRIDDEN")
    checkResult(finalState, executorResult, "OVERRIDDEN")
  }

}
