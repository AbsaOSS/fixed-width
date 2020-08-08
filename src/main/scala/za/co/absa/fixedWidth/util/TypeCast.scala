/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package za.co.absa.fixedWidth.util

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.types._
import za.co.absa.fixedWidth.{NullInNonNullableField, UnsupportedDataTypeCast}

private[fixedWidth] object TypeCast {
  /**
   * Casts given string datum to specified type.
   * We do not support complex types (ArrayType, MapType, StructType).
   *
   * @param datum string value
   * @param castType SparkSQL type
   */
  def castTo(fieldName: String,
             datum: String,
             castType: DataType,
             nullable: Boolean = true,
             treatEmptyValuesAsNulls: Boolean = false,
             nullValue: String = "",
             dateFormatter: Option[SimpleDateFormat] = None): Any = {

    val isValueNull = (datum == nullValue) || (treatEmptyValuesAsNulls && datum.isEmpty)

    if (isValueNull && nullable){
      null
    } else if (isValueNull && !nullable) {
      throw NullInNonNullableField(fieldName)
    }
    else {
      castType match {
        case _: ByteType =>
          datum.toByte
        case _: ShortType =>
          datum.toShort
        case _: IntegerType =>
          datum.toInt
        case _: LongType =>
          datum.toLong
        case _: FloatType =>
          datum.toFloat
        case _: DoubleType =>
          datum.toDouble
        case _: BooleanType =>
          datum.toBoolean
        case _: DecimalType =>
          new BigDecimal(datum)
        case _: TimestampType =>
          dateFormatter.map(_.parse(datum)).getOrElse(Timestamp.valueOf(datum))
        case _: DateType =>
          dateFormatter.map(_.parse(datum)).getOrElse(Date.valueOf(datum))
        case _: StringType =>
          datum
        case _ => throw UnsupportedDataTypeCast(fieldName, castType.typeName)
      }
    }
  }
}
