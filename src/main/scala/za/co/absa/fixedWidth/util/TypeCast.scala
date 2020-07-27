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
 */

package za.co.absa.fixedWidth.util

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Locale

import org.apache.spark.sql.types._

import scala.util.Try

private[fixedWidth] object TypeCast {
  /**
   * Casts given string datum to specified type.
   * We do not support complex types (ArrayType, MapType, StructType).
   *
   * @param datum string value
   * @param castType SparkSQL type
   */
  def castTo(datum: String,
             castType: DataType,
             nullable: Boolean = true,
             treatEmptyValuesAsNulls: Boolean = false,
             nullValue: String = "",
             dateFormatter: SimpleDateFormat = null): Any = {

    if ((datum == nullValue && nullable) || (treatEmptyValuesAsNulls && datum.isEmpty)){
      null
    } else {
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
          Try(datum.toFloat).getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
        case _: DoubleType =>
          Try(datum.toDouble).getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
        case _: BooleanType =>
          datum.toBoolean
        case _: DecimalType =>
          new BigDecimal(datum)
        case _: TimestampType if dateFormatter != null =>
          new Timestamp(dateFormatter.parse(datum).getTime)
        case _: TimestampType =>
          Timestamp.valueOf(datum)
        case _: DateType if dateFormatter != null =>
          new Date(dateFormatter.parse(datum).getTime)
        case _: DateType =>
          Date.valueOf(datum)
        case _: StringType =>
          datum
        case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
      }
    }
  }
}
