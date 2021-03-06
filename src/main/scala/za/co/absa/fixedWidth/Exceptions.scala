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

case class NullInNonNullableField(fieldName: String)
  extends Exception(s"Null found in non nullable field $fieldName")

case class UnsupportedDataTypeCast(fieldName: String, castType: String)
  extends Exception(s"Unsupported cast type of field $fieldName to type $castType")

case class SchemaValidationFailed(issueMessages: Seq[String])
  extends Exception(
    s"""Schema validation failed. Issues as follows:
       |${issueMessages.mkString("\n")}""".stripMargin)

case class ValidationException(str: String) extends Exception(str)

case class ValidationsException(validation: Seq[ValidationException]) extends Exception(
  s"""Parameter validations issues found. Issues are:
     |${validation.map(_.getMessage).mkString("\n")}""".stripMargin)
