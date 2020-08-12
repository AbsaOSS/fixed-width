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

package za.co.absa.fixedWidth

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.fixedWidth.util.SparkSessionTestWrapper

class FixedWidthRelationTest extends FunSuite with SparkSessionTestWrapper {
  private val filePath = getClass.getResource("/fixed-width.txt").getPath
  private val emptyFilePath = getClass.getResource("/emptyFile.txt").getPath

  private val width20Metadata = new MetadataBuilder()
    .putString("width", "20")
    .build()

  private val width3Metadata = new MetadataBuilder()
    .putLong("width", 3L)
    .build()

  private val defaultSchema = StructType(
    List(
      StructField("id", IntegerType, nullable = true, width3Metadata),
      StructField("Name", StringType, nullable = true, width20Metadata)
    )
  )

  private val defaultSchemaStrings = StructType(
    List(
      StructField("id", StringType, nullable = true, width3Metadata),
      StructField("Name", StringType, nullable = true, width20Metadata)
    )
  )

  test("Read FixedWidth all string") {
    val result = spark
      .read
      .format("fixed-width")
      .schema(defaultSchemaStrings)
      .load(filePath)

    assert(4 == result.collect().length)
    assert(defaultSchemaStrings == result.schema)
  }

  test("Read FixedWidth natural types") {
    val result = spark
      .read
      .format("fixed-width")
      .option("trimValues", "true")
      .schema(defaultSchema)
      .load(filePath)

    assert(4 == result.collect().length)
    assert(defaultSchema == result.schema)
  }

  test("Read FixedWidth empty") {
    val result = spark
      .read
      .format("fixed-width")
      .schema(defaultSchema)
      .load(emptyFilePath)

    assert(0 == result.collect().length)
    assert(defaultSchema == result.schema)
  }
}
