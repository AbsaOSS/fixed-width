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

import java.nio.charset.{Charset, UnsupportedCharsetException}

import org.apache.spark.sql.types._
import za.co.absa.fixedWidth.{SchemaValidationFailed, ValidationException, ValidationsException}

import scala.util.{Failure, Success, Try}

object FixedWidthValidations {
  private[fixedWidth] def validateSchema(schema: StructType): Unit = {
    if (schema == null) throw SchemaValidationFailed(Seq("Schema not provided"))

    val issueMessages = schema.fields.foldLeft(Seq.empty[String]) { (issues, field) =>
      if (field.metadata.contains("width")) {
        Try(SchemaUtils.getWidthValue(field)) match {
          case Success(_) => issues
          case Failure(exception) => issues :+ exception.getMessage
        }
      } else {
        issues :+ s"Column ${field.name} does not contain metadata width"
      }
    }

    if (issueMessages.nonEmpty) throw SchemaValidationFailed(issueMessages)
  }

  private[fixedWidth] def validateRead(parameters: Map[String, String]): Unit = {
    val validation = Seq(
      checkPath(parameters.get("path")),
      validateBooleanValues("trimValues", parameters.get("trimValues")),
      validateBooleanValues("treatEmptyValuesAsNulls", parameters.get("treatEmptyValuesAsNulls")),
      validateCharset(parameters.get("charset"))
    ).flatten

    println(validation.map(_.getMessage).mkString("\n"))

    if (validation.nonEmpty)
      throw ValidationsException(validation)
  }

  private[fixedWidth] def validateWrite(parameters: Map[String, String]): Unit = {
    val validation = Seq(checkPath(parameters.get("path"))).flatten

    if (validation.nonEmpty)
      throw ValidationsException(validation)
  }

  private def checkPath(path: Option[String]): Option[ValidationException] = {
    if(path.isEmpty || path.get == null || path.get.isEmpty)
      Some(ValidationException(s"Path to source either empty or not defined"))
    else None
  }

  private def validateBooleanValues(paramName: String, maybeString: Option[String]): Option[ValidationException] = {
    try {
      maybeString.map(_.toBoolean)
      None
    } catch {
      case e: IllegalArgumentException =>
        Some(ValidationException(s"Unable to parse $paramName option. It should be only true or false. Got $maybeString"))
    }
  }

  private def validateCharset(maybeString: Option[String]): Option[ValidationException] = {
    try {
      maybeString.map(Charset.forName)
      None
    } catch {
      case _: UnsupportedCharsetException =>
        Some(ValidationException(s"Unable to parse charset option. ${maybeString.get} is invalid"))
    }
  }
}
