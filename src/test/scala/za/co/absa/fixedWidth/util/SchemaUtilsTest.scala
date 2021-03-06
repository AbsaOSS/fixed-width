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

import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}
import org.scalatest.FunSuite

class SchemaUtilsTest extends FunSuite {
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

  private val negativeWidthMetadata = new MetadataBuilder()
    .putLong("width", -10)
    .putBoolean("bogusMetada", false)
    .build()

  private val structFieldString = StructField("randomColumn", StringType, metadata = stringMetadataGood)
  private val structFieldLong = StructField("randomColumn", StringType, metadata = longMetadataGood)
  private val structFieldNone = StructField("randomColumn", StringType, metadata = noWidthMetadata)
  private val structFieldNegativeWidth = StructField("randomColumn", StringType, metadata = negativeWidthMetadata)


  test("GetWidthValue - String parameter") {
    assert(10 == SchemaUtils.getWidthValue(structFieldString))
  }

  test("GetWidthValue - Long parameter") {
    assert(10 == SchemaUtils.getWidthValue(structFieldLong))
  }

  test("GetWidthValue - No parameter") {
    val expectedMsg = """Unable to parse metadata: width of column: randomColumn: {"bogusMetada":false}"""
    val msg = intercept[IllegalArgumentException] { SchemaUtils.getWidthValue(structFieldNone) }
    assert(expectedMsg == msg.getMessage)
  }

  test("GetWidthValue - Negative width parameter") {
    val expectedMsg = """Width of column randomColumn out of positive integer range. Width found -10"""
    val msg = intercept[IllegalArgumentException] { SchemaUtils.getWidthValue(structFieldNegativeWidth) }
    assert(expectedMsg == msg.getMessage)
  }

}
