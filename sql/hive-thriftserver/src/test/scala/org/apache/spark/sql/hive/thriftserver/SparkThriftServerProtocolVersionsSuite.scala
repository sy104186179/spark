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

import java.sql.{Date, Timestamp}
import java.util.Properties

import org.apache.hive.jdbc.{HiveConnection, HiveQueryResultSet}
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

class SparkThriftServerProtocolVersionsSuite extends HiveThriftServer2Test {

  override def mode: ServerMode.Value = ServerMode.binary

  def testWithProtocolVersion(
      version: ThriftserverShimUtils.TProtocolVersion,
      sql: String)(f: HiveQueryResultSet => Unit): Unit = {
    val rawTransport = new TSocket("localhost", serverPort)
    val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
    val user = System.getProperty("user.name")
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
    transport.open()
    var rs: HiveQueryResultSet = null
    try {
      val clientProtocol = new ThriftserverShimUtils.TOpenSessionReq(version)
      val openResp = client.OpenSession(clientProtocol)
      val sessHandle = openResp.getSessionHandle
      val execReq = new ThriftserverShimUtils.TExecuteStatementReq(sessHandle, sql)
      val execResp = client.ExecuteStatement(execReq)
      val stmtHandle = execResp.getOperationHandle

      // Set the HiveConnection protocol to our testing protocol version.
      // RowSetFactory uses this protocol version to construct different RowSet.
      val protocol = connection.getClass.getDeclaredField("protocol")
      protocol.setAccessible(true)
      protocol.set(connection, version)
      assert(connection.getProtocol === version)

      rs = new HiveQueryResultSet.Builder(connection)
        .setClient(client)
        .setSessionHandle(sessHandle)
        .setStmtHandle(stmtHandle).setMaxRows(Int.MaxValue).setFetchSize(Int.MaxValue)
        .build()
      f(rs)
    } finally {
      rs.close()
      connection.close()
      transport.close()
      rawTransport.close()
    }
  }

  ThriftserverShimUtils.testedProtocalVersions.foreach { version =>
    test(s"$version get byte type") {
      testWithProtocolVersion(version, "SELECT cast(1 as byte)") { rs =>
        assert(rs.next())
        assert(rs.getByte(1) === 1.toByte)
      }
    }

    test(s"$version get short type") {
      testWithProtocolVersion(version, "SELECT cast(1 as short)") { rs =>
        assert(rs.next())
        assert(rs.getShort(1) === 1.toShort)
      }
    }

    test(s"$version get int type") {
      testWithProtocolVersion(version, "SELECT 1") { rs =>
        assert(rs.next())
        assert(rs.getInt(1) === 1)
      }
    }

    test(s"$version get bigint type") {
      testWithProtocolVersion(version, "SELECT cast(1 as bigint)") { rs =>
        assert(rs.next())
        assert(rs.getLong(1) === 1L)
      }
    }

    test(s"$version get float type") {
      testWithProtocolVersion(version, "SELECT cast(1.2 as float)") { rs =>
        assert(rs.next())
        assert(rs.getFloat(1) === 1.2F)
      }
    }

    test(s"$version get double type") {
      testWithProtocolVersion(version, "SELECT cast(1.2 as double)") { rs =>
        assert(rs.next())
        assert(rs.getDouble(1) === 1.2D)
      }
    }

    // TODO: enable this test case after SPARK-28463 and SPARK-26969
    ignore(s"$version get decimal type") {
      testWithProtocolVersion(version, "SELECT cast(1 as decimal(18, 2)) as c") { rs =>
        assert(rs.next())
        assert(rs.getBigDecimal(1) === new java.math.BigDecimal("1.00"))
      }
    }

    test(s"$version get string type") {
      testWithProtocolVersion(version, "SELECT 'str'") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "str")
      }
    }

    test(s"$version get char type") {
      testWithProtocolVersion(version, "SELECT cast('char-str' as char(10))") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "char-str")
      }
    }

    test(s"$version get varchar type") {
      testWithProtocolVersion(version, "SELECT cast('varchar-str' as varchar(10))") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "varchar-str")
      }
    }

    // TODO: enable this test case after SPARK-28474
    ignore(s"$version get binary type") {
      testWithProtocolVersion(version, "SELECT cast('ABC' as binary)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "ABC")
      }
    }

    test(s"$version get boolean type") {
      testWithProtocolVersion(version, "SELECT true") { rs =>
        assert(rs.next())
        assert(rs.getBoolean(1) === true)
      }
    }

    test(s"$version get date type") {
      testWithProtocolVersion(version, "SELECT cast('2019-07-22' as date)") { rs =>
        assert(rs.next())
        assert(rs.getDate(1) === Date.valueOf("2019-07-22"))
      }
    }

    test(s"$version get timestamp type") {
      testWithProtocolVersion(version, "SELECT cast('2019-07-22 18:14:00' as timestamp)") { rs =>
        assert(rs.next())
        assert(rs.getTimestamp(1) === Timestamp.valueOf("2019-07-22 18:14:00"))
      }
    }

    // TODO: enable this test case after port HIVE-10646
    ignore(s"$version get void") {
      testWithProtocolVersion(version, "SELECT null") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === null)
      }
    }

    // We do not fully support interval type
    ignore(s"$version get interval type") {
      testWithProtocolVersion(version, "SELECT interval '1' year '2' day") { rs =>
        assert(rs.next())
      }
    }

    test(s"$version get array type") {
      testWithProtocolVersion(version, "SELECT array(1, 2)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "[1,2]")
      }
    }

    test(s"$version get map type") {
      testWithProtocolVersion(version, "SELECT map(1, 2)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === "{1:2}")
      }
    }

    test(s"$version get struct type") {
      testWithProtocolVersion(version, "SELECT struct('alpha' AS A, 'beta' AS B)") { rs =>
        assert(rs.next())
        assert(rs.getString(1) === """{"A":"alpha","B":"beta"}""")
      }
    }
  }
}
