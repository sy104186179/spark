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
import java.sql.{DriverManager, Statement, Timestamp}
import java.util.Locale

import scala.util.{Random, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class DebugThriftserverSuite extends QueryTest with SQLTestUtils
  with TestHiveSingleton with HiveThriftServer2Util {

  var hiveServer2: HiveThriftServer2 = _

  override def beforeEach(): Unit = {
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)
    diagnosisBuffer.clear()

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        Try(startThriftServer(listeningPort, attempt))
      }
    }.recover {
      case cause: Throwable =>
        dumpLogs()
        throw cause
    }.get
    logInfo(s"HiveThriftServer2 started successfully")
  }

  override def afterEach(): Unit = {
    hiveServer2.stop()
    hiveServer2 = null
  }

  private def startThriftServer(port: Int, attempt: Int): Unit = {
    logInfo(s"Trying to start HiveThriftServer2: port=$port, attempt=$attempt")
    val localSparkSession = spark.newSession()
    val sqlContext = localSparkSession.sqlContext
    sqlContext.setConf(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, port.toString)
    hiveServer2 = HiveThriftServer2.startWithContext(sqlContext)
  }

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

  test("SPARK-28620") {
    withJdbcStatement {
      statement =>
        val rs = statement.executeQuery(
          "select make_date(-44, 3, 15)")
        rs.next()
        val dd = HiveResult.toHiveString((rs.getObject(1), DateType))
        // scalastyle:off
        println(rs.getString(1))
      // scalastyle:on
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
    "blacklist.sql",   // Do NOT remove this one. It is here to test the blacklist functionality.
    "in-limit.sql", // Cannot recognize hive type string: decimal(2,-2)
    "date.sql", // SPARK-28624
    "aggregates_part1.sql", "group-by.sql", // SPARK-28619
    "float4.sql" // SPARK-28620
  )

  listTestCases().foreach(createScalaTestCase)

  private def runTest(testCase: TestCase): Unit = {
    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = input.split("\n").partition(_.startsWith("--"))
    val queies = code.map(f => if (f.contains("--")) f.split("--").head else f)

    // List of SQL queries to run
    // note: this is not a robust way to split queries using semicolon, but works for now.
    val queries = code.mkString("\n").split("(?<=[^\\\\]);").map(_.trim).filter(_ != "").toSeq

    runQueries(queries, testCase)
  }

  private def runQueries(queries: Seq[String], testCase: TestCase): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    withJdbcStatement { statement =>

      loadTestData(statement)

      testCase match {
        case _: PgSQLTest =>
          // PostgreSQL enabled cartesian product by default.
          statement.execute(s"SET ${SQLConf.CROSS_JOINS_ENABLED.key} = true")
          statement.execute(s"SET ${SQLConf.ANSI_SQL_PARSER.key} = true")
          statement.execute(s"SET ${SQLConf.PREFER_INTEGRAL_DIVISION.key} = true")
        case _ =>
      }

      // Run the SQL queries preparing them for comparison.
      val outputs: Seq[QueryOutput] = queries.map { sql =>
        val output = getNormalizedResult(statement, sql)
        // We might need to do some query canonicalization in the future.
        QueryOutput(
          sql = sql,
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
            output = if (isNeedSort(segments(i * 3 + 1).trim)) {
              segments(i * 3 + 3).split("\n").sorted.mkString("\n").trim
            } else {
              segments(i * 3 + 3).trim
            }
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

        expected match {
          case d if d.sql.toUpperCase(Locale.ROOT).startsWith("DESC ")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESC\n")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESCRIBE ")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESCRIBE\n") =>
          case s if s.sql.toUpperCase(Locale.ROOT).startsWith("SHOW ")
            || s.sql.toUpperCase(Locale.ROOT).startsWith("SHOW\n") =>
          case e if e.output.contains("AnalysisException")
            || output.output.contains("SQLException") =>
          case s if s.sql.equals("""select '\'', '"', '\n', '\r', '\t', 'Z'""") =>
          case s if s.sql.contains(
            "FROM (VALUES (100000003), (100000004), (100000006), (100000007)) v(x)") =>
          case _ =>
            assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
              output.output
            }
        }


        // Skip AnalysisException
//        if (!expected.sql.toUpperCase(Locale.ROOT).startsWith("DESC ")
//          && !expected.output.contains("AnalysisException")
        //          && !output.output.contains("SQLException")) {
//          assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
//            output.output
//          }
//        }
      }
    }
  }

  private def getNormalizedResult(statement: Statement, sql: String): Seq[String] = {
    try {
      val notIncludedMsg = "[not included in comparison]"
      val clsName = this.getClass.getCanonicalName

      val rs = statement.executeQuery(sql)
      val cols = rs.getMetaData.getColumnCount
      val buildStr = () => (for (i <- 1 to cols) yield {
        getHiveResult(rs.getObject(i))
      }).mkString("\t")

      val answer = Iterator.continually(rs.next()).takeWhile(identity).map(_ => buildStr()).toSeq
        .map(_.replaceAll("#\\d+", "#x")
          .replaceAll(
            s"Location.*/sql/core/spark-warehouse/$clsName/",
            s"Location ${notIncludedMsg}sql/core/spark-warehouse/")
          .replaceAll("Created By.*", s"Created By $notIncludedMsg")
          .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
          .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
          .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
          .replaceAll("\\*\\(\\d+\\) ", "*"))
      if (isNeedSort(sql)) {
        answer.sorted
      } else {
        answer
      }
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

  private def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
      testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      // Create a test case to run this case.
      test(testCase.name) {
        runTest (testCase)
      }
    }
  }


  /** A single SQL query's output. */
  private case class QueryOutput(sql: String, output: String) {
    def toString(queryIndex: Int): String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query $queryIndex\n" +
        sql + "\n" +
        s"-- !query $queryIndex output\n" +
        output
    }
  }

  private def listTestCases(): Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

      if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}udf")) {
        Seq.empty
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}pgSQL")) {
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
//    val filteredFiles = files.filter(f => f.getName.endsWith("int4.sql")
//      || f.getName.endsWith("float4.sql") || f.getName.endsWith("numeric.sql")
//      || f.getName.endsWith("boolean.sql") || f.getName.endsWith("aggregates_part1.sql")
//      || f.getName.endsWith("timestamp.sql") || f.getName.endsWith("cast.sql")
//      || f.getName.endsWith("float8.sql") || f.getName.endsWith("int8.sql"))


    // val filteredFiles = files.filter(f => f.getName.contains("group-by.sql"))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables. */
  private def loadTestData(statement: Statement): Unit = {
    // Prepare the data
    statement.execute(
      """
        |CREATE OR REPLACE TEMPORARY VIEW testdata as
        |SELECT id AS key, CAST(id AS string) AS value FROM range(1, 101)
      """.stripMargin)
    statement.execute(
      """
        |CREATE OR REPLACE TEMPORARY VIEW arraydata as
        |SELECT * FROM VALUES
        |(ARRAY(1, 2, 3), ARRAY(ARRAY(1, 2, 3))),
        |(ARRAY(2, 3, 4), ARRAY(ARRAY(2, 3, 4))) AS v(arraycol, nestedarraycol)
      """.stripMargin)
    statement.execute(
      """
        |CREATE OR REPLACE TEMPORARY VIEW mapdata as
        |SELECT * FROM VALUES
        |MAP(1, 'a1', 2, 'b1', 3, 'c1', 4, 'd1', 5, 'e1'),
        |MAP(1, 'a2', 2, 'b2', 3, 'c2', 4, 'd2'),
        |MAP(1, 'a3', 2, 'b3', 3, 'c3'),
        |MAP(1, 'a4', 2, 'b4'),
        |MAP(1, 'a5') AS v(mapcol)
      """.stripMargin)
    statement.execute(
      s"""
        |CREATE TEMPORARY VIEW aggtest
        |  (a int, b float)
        |USING csv
        |OPTIONS (path '${testFile("test-data/postgresql/agg.data")}',
        |  header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW onek
        |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
        |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
        |    stringu1 string, stringu2 string, string4 string)
        |USING csv
        |OPTIONS (path '${testFile("test-data/postgresql/onek.data")}',
        |  header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
        |CREATE OR REPLACE TEMPORARY VIEW tenk1
        |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
        |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
        |    stringu1 string, stringu2 string, string4 string)
        |USING csv
        |  OPTIONS (path '${testFile("test-data/postgresql/tenk.data")}',
        |  header 'false', delimiter '\t')
      """.stripMargin)
  }

  // Returns true if sql is retrieve.
  private def isNeedSort(sql: String): Boolean = {
    val removeComment = sql.split("\n").partition(_.trim.startsWith("--"))
      ._2.map(_.trim).filter(_ != "").mkString("\n").toUpperCase(Locale.ROOT)
    removeComment.startsWith("SELECT ") || removeComment.startsWith("SELECT\n") ||
      removeComment.startsWith("WITH ") || removeComment.startsWith("WITH\n") ||
      removeComment.startsWith("VALUES ") || removeComment.startsWith("VALUES\n") ||
      // pgSQL/union.sql
      removeComment.startsWith("(")
  }

  private def getHiveResult(obj: Object): String = {
    obj match {
      case null =>
        HiveResult.toHiveString((null, StringType))
      case d: java.sql.Date =>
        val ddd = d.toLocalDate.toEpochDay
        HiveResult.toHiveString((d, DateType))
      case t: Timestamp =>
        HiveResult.toHiveString((t, TimestampType))
      case d: java.math.BigDecimal =>
        HiveResult.toHiveString((d, DecimalType.fromBigDecimal(d)))
      case bin: Array[Byte] =>
        HiveResult.toHiveString((bin, BinaryType))
      case other =>
        other.toString
    }
  }
}
