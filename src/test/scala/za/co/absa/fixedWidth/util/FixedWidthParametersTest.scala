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

import java.nio.charset.UnsupportedCharsetException

import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}
import org.scalatest.FunSuite

class FixedWidthParametersTest extends FunSuite {
  private val stringMetadataGood = new MetadataBuilder()
    .putString("width", "10")
    .putBoolean("bogusMetada", false)
    .build()

  private val longMetadataGood = new MetadataBuilder()
    .putLong("width", 10)
    .putBoolean("bogusMetada", false)
    .build()

  private val noWidthMetadata = new MetadataBuilder()
    .putBoolean("bogusMetada", false)
    .build()

  private val structFieldString = StructField("randomColumn", StringType, metadata = stringMetadataGood)
  private val structFieldLong = StructField("randomColumn", StringType, metadata = longMetadataGood)
  private val structFieldNone = StructField("randomColumn", StringType, metadata = noWidthMetadata)

  test("ValidateRead - positive") {
    val params = Map(
      "path" -> "/alfa/beta",
      "trimValues" ->  "true",
      "treatEmptyValuesAsNulls" -> "false",
      "charset" -> "UTF-16")
    FixedWidthParameters.validateRead(params)
  }

  test("ValidateRead - missing path") {
    val expectedMsg = "Path to source either empty or not defined"
    val msg = intercept[IllegalStateException] { FixedWidthParameters.validateRead(Map("Something" ->  "else")) }
    assert(expectedMsg == msg.getMessage)
  }

  test("ValidateRead - bad value in trimValues") {
    val expectedMsg = "Unable to parse trimValues option. It should be only true or false"
    val msg = intercept[IllegalArgumentException] {
      FixedWidthParameters.validateRead(Map("path" -> "/alfa/beta", "trimValues" -> "blabla"))
    }
    assert(expectedMsg == msg.getMessage)
  }

  test("ValidateRead - unsupported charset") {
    val charset = "ALFA"
    val expectedMsg = s"Unable to parse charset option. $charset is invalid"
    val msg = intercept[UnsupportedCharsetException] {
      FixedWidthParameters.validateRead(Map("path" -> "/alfa/beta", "charset" -> charset))
    }
    assert(expectedMsg == msg.getMessage)
  }

  test("ValidateWrite - positive") {
    FixedWidthParameters.validateWrite(Map("path" -> "/alfa/beta", "Something" ->  "else"))
  }

  test("ValidateWrite - missing path") {
    val expectedMsg = "Path to source either empty or not defined"
    val msg = intercept[IllegalStateException] { FixedWidthParameters.validateWrite(Map("Something" ->  "else")) }
    assert(expectedMsg == msg.getMessage)
  }

  test("GetWidthValue - String parameter") {
    assert(10 == FixedWidthParameters.getWidthValue(structFieldString))
  }

  test("GetWidthValue - Long parameter") {
    assert(10 == FixedWidthParameters.getWidthValue(structFieldLong))
  }

  test("GetWidthValue - No parameter") {
    val expectedMsg = """Unable to parse metadata: width of column: randomColumn : {"bogusMetada":false}"""
    val msg = intercept[IllegalArgumentException] { FixedWidthParameters.getWidthValue(structFieldNone) }
    assert(expectedMsg == msg.getMessage)
  }

}
