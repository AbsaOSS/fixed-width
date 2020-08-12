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

import org.apache.spark.sql.types.StructField

import scala.util.{Success, Try}

object SchemaUtils {
  private[fixedWidth] def getWidthValue(field: StructField): Int = {
    val metadataKey = "width"
    val maybeLong = Try(field.metadata.getLong(metadataKey)).recover{
      case _ => field.metadata.getString(metadataKey).toLong
    }

    maybeLong match {
      case Success(value) if (value >= 1) && (value <= Int.MaxValue) => value.toInt
      case Success(value) => throw new IllegalArgumentException(
        s"Width of column ${field.name} out of positive integer range. Width found $value"
      )
      case _ => throw new IllegalArgumentException(
        s"Unable to parse metadata: width of column: ${field.name}: ${field.metadata.toString()}"
      )
    }
  }
}
