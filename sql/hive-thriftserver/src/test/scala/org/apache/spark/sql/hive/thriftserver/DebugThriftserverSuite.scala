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

package org.apache.spark.sql.hive.thriftserver

import java.io.File
import java.sql.{DriverManager, Statement}
import java.util.Locale

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

// scalastyle:off
import scala.concurrent.duration._
import scala.util.{Random, Try}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.hive.test.{HiveTestUtils, TestHiveSingleton}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

import scala.util.control.NonFatal

class DebugThriftserverSuite extends QueryTest with SQLTestUtils with TestHiveSingleton with HiveThriftServer2Util {

  var localSparkSession: SparkSession = _
  var hiveServer2: HiveThriftServer2 = _

  override def beforeEach(): Unit = {
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)
    diagnosisBuffer.clear()

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        // listeningPort += 1
        // localSparkSession.stop()
        Try(startThriftServer(listeningPort, attempt))
      }
    }.recover {
      case cause: Throwable =>
        dumpLogs()
        throw cause
    }.get
    logInfo(s"HiveThriftServer2 started successfully")
  }

//  override def beforeAll(): Unit = {
//
//  }

  private def startThriftServer(port: Int, attempt: Int) = {
    logInfo(s"Trying to start HiveThriftServer2: port=$port, attempt=$attempt")
    localSparkSession = spark.newSession()
    val sqlContext = localSparkSession.sqlContext
    sqlContext.setConf(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, port.toString)
    hiveServer2 = HiveThriftServer2.startWithContext(sqlContext)
  }

//  spark.sqlContext.setConf("hive.server2.thrift.port", "10001")
//  HiveThriftServer2.startWithContext(spark.sqlContext)

  override def afterEach(): Unit = {
    hiveServer2.stop()
    hiveServer2 = null
  }

  Utils.classForName(classOf[HiveDriver].getCanonicalName)
  def withJdbcStatement(fs: (Statement => Unit)*) {
    val user = System.getProperty("user.name")

    val serverPort = hiveServer2.getHiveConf.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname)
    val connections =
      fs.map { _ => DriverManager.getConnection(s"jdbc:hive2://localhost:$serverPort", user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  test("123") {
    withJdbcStatement {
        statement =>
          val rs = statement.executeQuery("select null")
          rs.next()
          // scalastyle:off
          println(rs.getString(1))
      }
  }

  // begin

  private val baseResourcePath = {
    val res = getClass.getClassLoader.getResource("sql-tests")
    new File(res.getFile)
  }

  private val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  private val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  private val validFileExtensions = ".sql"

  /** List of test cases to ignore, in lower cases. */
  private val blackList = Set(
    "blacklist.sql"   // Do NOT remove this one. It is here to test the blacklist functionality.
  )

  listTestCases().foreach(createScalaTestCase)

  private def runTest(testCase: TestCase): Unit = {
    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = input.split("\n").partition(_.startsWith("--"))

    // List of SQL queries to run
    // note: this is not a robust way to split queries using semicolon, but works for now.
    val queries = code.mkString("\n").split("(?<=[^\\\\]);").map(_.trim).filter(_ != "").toSeq

    // scalastyle:off
    println(code)
    println("======")
    println(queries.mkString(","))

    runQueries(queries, testCase, None)
  }

  private def runQueries(
                          queries: Seq[String],
                          testCase: TestCase,
                          configSet: Option[Seq[(String, String)]]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    //    val localSparkSession = spark.newSession()
    //    loadTestData(localSparkSession)

    withJdbcStatement { statement =>

      loadTestData(statement)

      // Run the SQL queries preparing them for comparison.
      val outputs: Seq[QueryOutput] = queries.map { sql =>
        val output = getNormalizedResult(statement, sql)
        // We might need to do some query canonicalization in the future.
        QueryOutput(
          sql = sql,
          schema = StructType(Seq.empty).catalogString,
          output = output.mkString("\n").trim)
      }

      // Read back the golden file.
      val expectedOutputs: Seq[QueryOutput] = {
        val goldenOutput = fileToString(new File(testCase.resultFile))
        val segments = goldenOutput.split("-- !query.+\n")

        // each query has 3 segments, plus the header
        assert(segments.size == outputs.size * 3 + 1,
          s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            s"Try regenerate the result files.")
        Seq.tabulate(outputs.size) { i =>
          QueryOutput(
            sql = segments(i * 3 + 1).trim,
            schema = segments(i * 3 + 2).trim,
            output = segments(i * 3 + 3).split("\n").sorted.mkString("\n").trim
          )
        }
      }

      // Compare results.
      assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
        outputs.size
      }

      outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }
        // Skip AnalysisException
        if (!expected.output.contains("AnalysisException") && !output.output.contains("SQLException")) {
          assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
            output.output
          }
        }
      }
    }
  }

  private def getNormalizedResult(statement: Statement, sql: String): Seq[String] = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeCommandBase | _: DescribeColumnCommand => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    try {
      hiveServer2.getServices
      // scalastyle:off
      println(sql)
      // statement.executeQuery(sql)
      //      val df = session.sql(sql)
      //      val schema = df.schema
      val notIncludedMsg = "[not included in comparison]"
      val clsName = this.getClass.getCanonicalName
      // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
      //      val answer = hiveResultString(df.queryExecution.executedPlan)
      //        .map(_.replaceAll("#\\d+", "#x")
      //          .replaceAll(
      //            s"Location.*/sql/core/spark-warehouse/$clsName/",
      //            s"Location ${notIncludedMsg}sql/core/spark-warehouse/")
      //          .replaceAll("Created By.*", s"Created By $notIncludedMsg")
      //          .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
      //          .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
      //          .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
      //          .replaceAll("\\*\\(\\d+\\) ", "*"))  // remove the WholeStageCodegen codegenStageIds

      // If the output is not pre-sorted, sort it.
      //      if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)

      val isDFSorted = if(!sql.toLowerCase(Locale.ROOT).startsWith("create")) {
        try {
         isSorted(localSparkSession.sql(sql).queryExecution.analyzed)
        } catch {
          case NonFatal(e) =>
            false
        }
      } else {
        false
      }

      val rs = statement.executeQuery(sql)
      val cols = rs.getMetaData.getColumnCount
      val buildStr = () => (for (i <- 1 to cols) yield {
        val result = rs.getString(i)
        if (result == null) {
          // BeeLineOpts.DEFAULT_NULL_STRING
          "NULL"
        } else {
          result
        }
      }).mkString("\t")
      val answer = Iterator.continually(rs.next()).takeWhile(identity).map(_ => buildStr()).toSeq

      answer.sorted
    } catch {
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        val msg = if (a.plan.nonEmpty) a.getSimpleMessage else a.getMessage
        Seq(a.getClass.getName, msg.replaceAll("#\\d+", "#x")).sorted
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        Seq(e.getClass.getName, e.getMessage).sorted
    }
  }

  /** A test case. */
  private trait TestCase {
    val name: String
    val inputFile: String
    val resultFile: String
  }

  /**
   * traits that indicate UDF or PgSQL to trigger the code path specific to each. For instance,
   * PgSQL tests require to register some UDF functions.
   */
  private trait PgSQLTest

  /** A regular test case. */
  private case class RegularTestCase(
                                      name: String, inputFile: String, resultFile: String) extends TestCase

  /** A PostgreSQL test case. */
  private case class PgSQLTestCase(
                                    name: String, inputFile: String, resultFile: String) extends TestCase with PgSQLTest

  /** A UDF test case. */
  private case class UDFTestCase(
                                  name: String,
                                  inputFile: String,
                                  resultFile: String) extends TestCase

  /** A UDF PostgreSQL test case. */
  private case class UDFPgSQLTestCase(
                                       name: String,
                                       inputFile: String,
                                       resultFile: String) extends TestCase with PgSQLTest

  private def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
      testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else{
      // Create a test case to run this case.
      test(testCase.name) {
        runTest (testCase)
      }
    }
  }


  /** A single SQL query's output. */
  private case class QueryOutput(sql: String, schema: String, output: String) {
    def toString(queryIndex: Int): String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query $queryIndex\n" +
        sql + "\n" +
        s"-- !query $queryIndex schema\n" +
        schema + "\n" +
        s"-- !query $queryIndex output\n" +
        output
    }
  }

  private def listTestCases(): Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

     if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}pgSQL")) {
        PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      }
    }
  }

  /** Returns all the files (not directories) in a directory, recursively. */
  private def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables into the SparkSession. */
  private def loadTestData(statement: Statement): Unit = {
    // Prepare the data
    statement.execute("CREATE OR REPLACE TEMPORARY VIEW testdata as SELECT id AS key, CAST(id AS string) AS value FROM range(1, 101)")
    statement.execute("CREATE OR REPLACE TEMPORARY VIEW arraydata as SELECT * FROM VALUES (ARRAY(1, 2, 3), ARRAY(ARRAY(1, 2, 3))), (ARRAY(2, 3, 4), ARRAY(ARRAY(2, 3, 4))) AS v(arraycol, nestedarraycol)")
    statement.execute("CREATE OR REPLACE TEMPORARY VIEW mapdata as SELECT * FROM VALUES MAP(1, 'a1', 2, 'b1', 3, 'c1', 4, 'd1', 5, 'e1'), MAP(1, 'a2', 2, 'b2', 3, 'c2', 4, 'd2'), MAP(1, 'a3', 2, 'b3', 3, 'c3'), MAP(1, 'a4', 2, 'b4'),  MAP(1, 'a5') AS v(mapcol)")
    statement.execute(
      s"""
        |CREATE TEMPORARY VIEW aggtest
        |  (a int, b float)
        |USING csv
        |OPTIONS (path '${testFile("test-data/postgresql/agg.data")}', header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW onek
        |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
        |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
        |    stringu1 string, stringu2 string, string4 string)
        |USING csv
        |  OPTIONS (path '${testFile("test-data/postgresql/onek.data")}', header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW tenk1
        |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
        |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
        |    stringu1 string, stringu2 string, string4 string)
        |USING csv
        |  OPTIONS (path '${testFile("test-data/postgresql/tenk.data")}', header 'false', delimiter '\t')
      """.stripMargin)
  }


}
