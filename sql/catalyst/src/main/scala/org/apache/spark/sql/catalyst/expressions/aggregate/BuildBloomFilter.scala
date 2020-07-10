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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the mean calculated from values of a group.",
  examples = """
    Examples:
      > SELECT in_bloom_filter(t.bf, 2) FROM (SELECT build_bloom_filter(col) as bf FROM VALUES (1), (2), (3) AS tab(col)) t;
       true
      > SELECT in_bloom_filter(t.bf, 2) FROM (SELECT build_bloom_filter(col) as bf FROM VALUES (1), (2), (3) AS tab(col)) t;
       false
  """,
  group = "agg_funcs",
  since = "3.1.0")
// scalastyle:on line.size.limit
case class BuildBloomFilter(
    child: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BloomFilter] with ExpectsInputTypes {

  def this(child: Expression) = {
    this(child, 0, 0)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(IntegralType, StringType, BinaryType, FloatType, DoubleType,
      DateType, TimestampType))
  }

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType


  override def createAggregationBuffer(): BloomFilter = {
    BloomFilter.create(10000, 10)
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      value match {
        case d: Float => buffer.putLong(java.lang.Float.floatToIntBits(d).toLong)
        case d: Double => buffer.putLong(java.lang.Double.doubleToLongBits(d))
        case i: Number => buffer.putLong(i.longValue())
        case s: UTF8String => buffer.putBinary(s.getBytes)
        case a: Array[Byte] => buffer.putBinary(a)
      }
    }
    buffer
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    buffer.mergeInPlace(input)
    buffer
  }

  override def eval(buffer: BloomFilter): Any = serialize(buffer)

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    val result = new ByteArrayOutputStream
    buffer.writeTo(result)
    result.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): BloomFilter = {
    BloomFilter.readFrom(new ByteArrayInputStream(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): BuildBloomFilter =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): BuildBloomFilter =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  override def prettyName: String = "build_bloom_filter"
}
