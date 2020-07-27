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

import org.apache.spark.sql.types.StructField

import scala.util.control.NonFatal

object FixedWidthParameters {

  private def checkPath(path: Option[String]): Unit = {
    if(path.isEmpty || path.get == null || path.get.isEmpty)
      throw new IllegalStateException(s"Path to source either empty or not defined")
  }

  private[fixedWidth] def validateRead(parameters: Map[String, String]): Unit = {
    checkPath(parameters.get("path"))

    try {
      parameters.getOrElse("trimValues", "false").toBoolean
    } catch {
      case _: Exception => throw new IllegalArgumentException("trimValues option should be only true or false")
    }
  }

  private[fixedWidth] def validateWrite(parameters: Map[String, String]): Unit = {
    checkPath(parameters.get("path"))
  }

  private[fixedWidth] def getWidthValue(field: StructField): Int = {
    try {
      val width = field.metadata.getString("width")
      width.toInt
    }
    catch {
      case NonFatal(_) => throw new IllegalArgumentException(
        s"Unable to parse metadata: width of column: ${field.name} : ${field.metadata.toString()}"
      )
    }
  }
}
