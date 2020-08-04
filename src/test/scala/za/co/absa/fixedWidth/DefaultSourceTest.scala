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
import za.co.absa.fixedWidth.util.{SparkSessionTestWrapper, TextFile}

class DefaultSourceTest extends FunSuite with SparkSessionTestWrapper {
  private val defaultSource = new DefaultSource

  private val expectedText =
    """A guy asks a girl to go to a dance. She agrees, and he decides to rent a suit. The rental has a long line, so he waits and waits, and finally he gets his suit.
      |He decides to buy flowers, so he goes to the flower shop. The flower shop has a long line, so he waits and waits, until he finally buys flowers.
      |He picks up the girl and they go to the dance. There is a long line into the dance, so they wait and wait.
      |Finally, they get into the dance, and the guy offers to get the girl a drink. She asks for punch, so he goes to the drink table, and there is no punch line.""".stripMargin

  private val filePath = getClass.getResource("/testTextFile.txt").getPath

  private val defaultSchema = StructType(
    List(
      StructField("num", IntegerType, true),
      StructField("letter", StringType, true)
    )
  )

  private val paramasFull = Map[String, String](
    "path" -> filePath,
    "trimValues" -> "true",
    "dateFormat" -> "yy-MM-dd HH:mm",
    "mode" -> "PERMISSIVE",
    "charset" -> "ASCII",
    "treatEmptyValuesAsNulls" -> "true",
    "nullValue" -> "Alfa",
    "Extra" -> "Extra"
  )

  private val fixedWidthRelationFull = FixedWidthRelation(
    () => TextFile.withCharset(spark.sqlContext.sparkContext, filePath, "ASCII"),
    defaultSchema,
    trimValues = true,
    Some("yy-MM-dd HH:mm"),
    "PERMISSIVE",
    treatEmptyValuesAsNulls = true,
    "Alfa"
  )(spark.sqlContext)


  test("CreateRelation - full params") {
    val baseRelation = defaultSource
      .createRelation(spark.sqlContext, paramasFull, defaultSchema)
      .asInstanceOf[FixedWidthRelation]
    assert(fixedWidthRelationFull.baseRDD.apply().collect() sameElements baseRelation.baseRDD.apply().collect())
    assert(fixedWidthRelationFull.userSchema == baseRelation.userSchema)
    assert(fixedWidthRelationFull.trimValues == baseRelation.trimValues)
    assert(fixedWidthRelationFull.dateFormat == baseRelation.dateFormat)
    assert(fixedWidthRelationFull.parseMode == baseRelation.parseMode)
    assert(fixedWidthRelationFull.treatEmptyValuesAsNulls == baseRelation.treatEmptyValuesAsNulls)
    assert(fixedWidthRelationFull.nullValue == baseRelation.nullValue)
  }

  test("CreateRelation - minimal params") {
    val baseRelation = defaultSource
      .createRelation(spark.sqlContext, Map("path" -> filePath))
      .asInstanceOf[FixedWidthRelation]
    assert(fixedWidthRelationFull.baseRDD.apply().collect() sameElements baseRelation.baseRDD.apply().collect())
    assert(null == baseRelation.userSchema)
    assert(!baseRelation.trimValues)
    assert(baseRelation.dateFormat.isEmpty)
    assert("FAILFAST" == baseRelation.parseMode)
    assert(!baseRelation.treatEmptyValuesAsNulls)
    assert("" == baseRelation.nullValue)
  }

}
