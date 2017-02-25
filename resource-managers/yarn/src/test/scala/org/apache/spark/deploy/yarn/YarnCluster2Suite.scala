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
class YarnClusterSuite2 extends BaseYarnClusterSuite {

  override def newYarnConfig(): YarnConfiguration = new YarnConfiguration()

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



  test("run Spark in yarn-cluster mode with using SparkHadoopUtil.conf") {
    testYarnAppUseSparkHadoopUtilConf2()
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

  private def testYarnAppUseSparkHadoopUtilConf2(): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(false,
      mainClassName(YarnClusterDriverUseSparkHadoopUtilConf2.getClass),
      appArgs = Seq("key=value", result.getAbsolutePath()),
      extraConf = Map("spark.hadoop.key" -> "value"))
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

    val sc = new SparkContext(new SparkConf()
      .set("spark.extraListeners", classOf[SaveExecutorInfo].getName)
      .setAppName("yarn \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns"))
    val conf = sc.getConf
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