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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import za.co.absa.fixedWidth.util.{FixedWidthParameters, TextFile}

/**
 * Provides access to FixedWidth data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {
  override def shortName(): String = "fixed-width"

  /**
   * This method is here only for compatibility reasons. Fixed width expects metadata width in schema for each column.
   * TODO make width a one off parameter
   *
   * Creates a new relation for data store in FixedWidth given parameters.
   * Parameters have to include 'path' and optionally 'trimValues'
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in FixedWidth given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'trimValues'. Schema metadata has to include width.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    FixedWidthParameters.validateRead(parameters)
    FixedWidthParameters.validateSchema(schema)

    val path = parameters("path")
    val trimValues = parameters.getOrElse("trimValues", "false").toBoolean
    val dateFormat = parameters.get("dateFormat")
    val parseMode = parameters.getOrElse("mode", "FAILFAST")
    val charset = parameters.getOrElse("charset", TextFile.DEFAULT_CHARSET.name())
    val treatEmptyValuesAsNulls = parameters.getOrElse("treatEmptyValuesAsNulls", "false").toBoolean
    val nullValue = parameters.getOrElse("nullValue", "")

    FixedWidthRelation(
      () => TextFile.withCharset(sqlContext.sparkContext, path, charset),
      schema,
      trimValues,
      dateFormat,
      parseMode,
      treatEmptyValuesAsNulls,
      nullValue)(sqlContext)
  }
}
