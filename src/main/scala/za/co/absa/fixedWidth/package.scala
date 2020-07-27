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

package za.co.absa

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import za.co.absa.fixedWidth.util.TextFile

package object fixedWidth {

  /**
   * Adds a method, `fixedWidthFile`, to SQLContext that allows reading FixedWidth data.
   */
  implicit class FixedWidthContext(sqlContext: SQLContext) extends Serializable {
    def fixedWidthFile(
                 filePath: String,
                 schema: StructType,
                 mode: String = "FAILFAST",
                 trimValues: Boolean = true,
                 dateFormat: String = null,
                 nullValue: String = "",
                 charset: String = TextFile.DEFAULT_CHARSET.name()): DataFrame = {
      val csvRelation = FixedWidthRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        userSchema = schema,
        trimValues = trimValues,
        dateFormat = dateFormat,
        parseMode = mode,
        treatEmptyValuesAsNulls = false,
        nullValue = nullValue)(sqlContext)

      sqlContext.baseRelationToDataFrame(csvRelation)
    }
  }
}
