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

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory
import za.co.absa.fixedWidth.util._

import scala.util.control.NonFatal

case class FixedWidthRelation(baseRDD: () => RDD[String],
                              userSchema: StructType,
                              trimValues: Boolean,
                              dateFormat: Option[String] = None,
                              parseMode: String = "PERMISSIVE",
                              treatEmptyValuesAsNulls: Boolean = false,
                              nullValue: String = "")
                             (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with Serializable
    with TableScan {

  type ColumnWidth = (Int, Int)

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val failFast = ParseModes.isFailFastMode(parseMode)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val dateFormatter: Option[SimpleDateFormat] = dateFormat.map(new SimpleDateFormat(_))

  override def schema: StructType = userSchema

  override def buildScan(): RDD[Row] = {
    val schemaFields = schema.fields.zipWithIndex
    val columnWidths = getWidthArray(schema.fields)

    baseRDD().mapPartitions { iter =>
      parsePartitions(iter, columnWidths).flatMap { row =>
        parseRow(schemaFields, row)
      }
    }
  }

  private def parseRow(schemaFields: Array[(StructField, Int)], row: Seq[String]): Option[Row] = {
    if (dropMalformed && schemaFields.length != row.length) {
      logger.warn(s"Dropping malformed line: ${row.mkString(",")}")
      None
    } else if (failFast && schemaFields.length != row.length) {
      throw new RuntimeException(s"Malformed line in FAILFAST mode: ${row.mkString("\t")}")
    } else {
      val typedRow = schemaFields.map { case (field, index) =>
        TypeCast.castTo(
          field.name,
          row(index),
          field.dataType,
          field.nullable,
          treatEmptyValuesAsNulls,
          nullValue,
          dateFormatter)
      }

      Some(Row.fromSeq(typedRow))
    }
  }

  private def parsePartitions(iter: Iterator[String], columnWidths: Seq[ColumnWidth]): Iterator[Seq[String]] = {
    iter.flatMap { line =>
      try {
        Some(columnWidths.map { width =>
          val value = line.substring(width._1, width._2.min(line.length))
          optionallyTrimValues(value)
        })
      } catch {
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

  private def getWidthArray(fields: Array[StructField]): Seq[ColumnWidth] = {
    fields.foldLeft(Seq.empty[ColumnWidth]) { (acc, field) =>
      val index = acc.lastOption.getOrElse((0, 0))._2
      if (field.metadata.contains("width")) {
        val width = SchemaUtils.getWidthValue(field)
        val length = index + width
        val columnWidth: ColumnWidth = (index, length)
        acc :+ columnWidth
      } else {
        throw new IllegalStateException(s"No width has been defined for the column ${field.name}")
      }
    }
  }

  private def optionallyTrimValues(value: String): String = {
    if (trimValues) {
      value.trim
    } else {
      value
    }
  }
}

