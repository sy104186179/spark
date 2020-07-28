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

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[BaseSubqueryExec] {
  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit

  /** Updates the expression with a new plan. */
  override def withNewPlan(plan: BaseSubqueryExec): ExecSubqueryExpression
}

object ExecSubqueryExpression {
  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.find {
      case _: ExecSubqueryExpression => true
      case _ => false
    }.isDefined
  }
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): ScalarSubquery = copy(plan = query)

  override def semanticEquals(other: Expression): Boolean = other match {
    case s: ScalarSubquery => plan.sameResult(s.plan)
    case _ => false
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      sys.error(s"more than one row returned by a subquery used as an expression:\n$plan")
    }
    if (rows.length == 1) {
      assert(rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      result = rows(0).get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}

/**
 * The physical node of in-subquery. This is for Dynamic Partition Pruning only, as in-subquery
 * coming from the original query will always be converted to joins.
 */
case class InSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    private var resultBroadcast: Broadcast[Array[Any]] = null) extends ExecSubqueryExpression {

  @transient private var result: Array[Any] = _

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = child.nullable
  override def toString: String = s"$child IN ${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): InSubqueryExec = copy(plan = plan)

  override def semanticEquals(other: Expression): Boolean = other match {
    case in: InSubqueryExec => child.semanticEquals(in.child) && plan.sameResult(in.plan)
    case _ => false
  }

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = child.dataType match {
      case _: StructType => rows.toArray
      case _ => rows.map(_.get(0, child.dataType))
    }
    resultBroadcast = plan.sqlContext.sparkContext.broadcast(result)
  }

  def values(): Option[Array[Any]] = Option(resultBroadcast).map(_.value)

  private def prepareResult(): Unit = {
    require(resultBroadcast != null, s"$this has not finished")
    if (result == null) {
      result = resultBroadcast.value
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    val v = child.eval(input)
    if (v == null) {
      null
    } else {
      result.contains(v)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()
    InSet(child, result.toSet).doGenCode(ctx, ev)
  }

  override lazy val canonicalized: InSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null)
  }
}

case class BloomFilterSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    private var resultBroadcast: Broadcast[BloomFilter] = null) extends ExecSubqueryExpression {

  @transient private var bloomFilter: BloomFilter = _

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = true
  override def toString: String = s"BloomFilter($child, ${plan.name})"
  override def withNewPlan(plan: BaseSubqueryExec): BloomFilterSubqueryExec = copy(plan = plan)

  override def semanticEquals(other: Expression): Boolean = other match {
    case bf: BloomFilterSubqueryExec => child.semanticEquals(bf.child) && plan.sameResult(bf.plan)
    case _ => false
  }

  override def updateResult(): Unit = {
    val rows = plan.executeCollect().filterNot(_.anyNull)
    bloomFilter = BloomFilter.create(math.max(1, rows.length))

    child.dataType match {
      case BooleanType =>
        rows.foreach(r => bloomFilter.putLong(JBoolean.compare(r.getBoolean(0), false).toLong))
      case ByteType =>
        rows.foreach(r => bloomFilter.putLong(r.getByte(0).toLong))
      case ShortType =>
        rows.foreach(r => bloomFilter.putLong(r.getShort(0).toLong))
      case IntegerType | DateType =>
        rows.foreach(r => bloomFilter.putLong(r.getInt(0).toLong))
      case LongType | TimestampType =>
        rows.foreach(r => bloomFilter.putLong(r.getLong(0)))
      case FloatType =>
        rows.foreach(r => bloomFilter.putLong(JFloat.floatToIntBits(r.getFloat(0)).toLong))
      case DoubleType =>
        rows.foreach(r => bloomFilter.putLong(JDouble.doubleToLongBits(r.getDouble(0))))
      case StringType =>
        rows.foreach(r => bloomFilter.putBinary(r.getUTF8String(0).getBytes))
      case BinaryType =>
        rows.foreach(r => bloomFilter.putBinary(r.getBinary(0)))
      case d: DecimalType =>
        rows.foreach(r => bloomFilter.putBinary(r.getDecimal(0, d.precision, d.scale)
          .toJavaBigDecimal.unscaledValue().toByteArray))
      case _ =>
    }

    resultBroadcast = plan.sqlContext.sparkContext.broadcast(bloomFilter)
  }

  private def prepareResult(): Unit = {
    require(resultBroadcast != null, s"$this has not finished")
    if (bloomFilter == null) {
      bloomFilter = resultBroadcast.value
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    val v = child.eval(input)
    if (v == null) {
      null
    } else {
      child.dataType match {
        case BooleanType =>
          bloomFilter.mightContainLong(JBoolean.compare(v.asInstanceOf[Boolean], false).toLong)
        case _: IntegralType | DateType | TimestampType =>
          bloomFilter.mightContainLong(v.asInstanceOf[Number].longValue())
        case FloatType =>
          bloomFilter.mightContainLong(JFloat.floatToIntBits(v.asInstanceOf[Float]).toLong)
        case DoubleType =>
          bloomFilter.mightContainLong(JDouble.doubleToLongBits(v.asInstanceOf[Double]))
        case StringType =>
          bloomFilter.mightContainBinary(v.asInstanceOf[UTF8String].getBytes)
        case BinaryType =>
          bloomFilter.mightContainBinary(v.asInstanceOf[Array[Byte]])
        case _: DecimalType =>
          bloomFilter.mightContainBinary(v.asInstanceOf[Decimal]
            .toJavaBigDecimal.unscaledValue().toByteArray)
        case _ =>
          // Always return true for unsupported data type.
          true
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()

    val childCode = child.genCode(ctx)
    val input = childCode.value

    val bf = ctx.addReferenceObj("bloomFilter", bloomFilter)
    val testCode = child.dataType match {
      case BooleanType => s"$bf.mightContainLong((long)Boolean.compare($input, false))"
      case _: IntegralType | DateType | TimestampType => s"$bf.mightContainLong((long)$input)"
      case FloatType => s"$bf.mightContainLong((long)Float.floatToIntBits($input))"
      case DoubleType => s"$bf.mightContainLong(Double.doubleToLongBits($input))"
      case StringType => s"$bf.mightContainBinary($input.getBytes())"
      case BinaryType => s"$bf.mightContainBinary($input)"
      case _: DecimalType =>
        s"$bf.mightContainBinary($input.toJavaBigDecimal().unscaledValue().toByteArray())"
      case _ => true.toString
    }

    ev.copy(code = childCode.code +
      code"""
            |boolean ${ev.value} = true;
            |boolean ${ev.isNull} = ${childCode.isNull};
            |if (!${childCode.isNull}) {
            |  ${ev.value} = $testCode;
            |}
      """.stripMargin)
  }

  override lazy val canonicalized: BloomFilterSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null)
  }
}

/**
 * Plans subqueries that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, subquery.plan)
        ScalarSubquery(
          SubqueryExec(s"scalar-subquery#${subquery.exprId.id}", executedPlan),
          subquery.exprId)
      case expressions.InSubquery(values, ListQuery(query, _, exprId, _)) =>
        val expr = if (values.length == 1) {
          values.head
        } else {
          CreateNamedStruct(
            values.zipWithIndex.flatMap { case (v, index) =>
              Seq(Literal(s"col_$index"), v)
            }
          )
        }
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, query)
        InSubqueryExec(expr, SubqueryExec(s"subquery#${exprId.id}", executedPlan), exprId)
    }
  }
}

/**
 * Find out duplicated subqueries in the spark plan, then use the same subquery result for all the
 * references.
 */
case class ReuseSubquery(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.subqueryReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of subqueries to avoid O(N*N) sameResult calls.
    val subqueries = mutable.HashMap[StructType, ArrayBuffer[BaseSubqueryExec]]()
    plan transformAllExpressions {
      case sub: ExecSubqueryExpression =>
        val sameSchema =
          subqueries.getOrElseUpdate(sub.plan.schema, ArrayBuffer[BaseSubqueryExec]())
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          sub.withNewPlan(ReusedSubqueryExec(sameResult.get))
        } else {
          sameSchema += sub.plan
          sub
        }
    }
  }
}
