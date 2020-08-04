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

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.fixedWidth.{NullInNonNullableField, UnsupportedDataTypeCast}

class TypeCastTest extends FunSuite {
  private val nullable = true
  private val nonNullable = false
  private val fieldName = "RandomFieldName"

  test("CastTo - NullValue") {
    val casted = TypeCast.castTo(fieldName,"alfa", ByteType, nullable, nullValue = "alfa")
    assert(casted == null)
  }

  test("CastTo - NullValue in non nullable") {
    val expectedMsg = "Null found in non nullable field RandomFieldName"
    val msg = intercept[NullInNonNullableField] {
      TypeCast.castTo(
        fieldName,
        datum = "", DoubleType,
        nonNullable,
        treatEmptyValuesAsNulls = true,
        nullValue = "alfa")
    }
    assert(expectedMsg == msg.getMessage)
  }

  test("CastTo - Long") {
    val casted = TypeCast.castTo(fieldName, "999999", LongType)
    assert(casted == 999999L)
  }

  test("CastTo - Date with pattern") {
    val dateFormatter = Some(new SimpleDateFormat("yyyyMMdd"))
    val casted = TypeCast.castTo(fieldName, "20000101", DateType, dateFormatter = dateFormatter)
    assert(Date.valueOf("2000-01-01") == casted)
  }

  test("CastTo - Usuported type") {
    val expectedMsg = "Unsupported cast type of field RandomFieldName to type calendarinterval"
    val msg = intercept[UnsupportedDataTypeCast] {
      TypeCast.castTo(fieldName, "20000101", CalendarIntervalType)
    }
    assert(expectedMsg == msg.getMessage)
  }

}
