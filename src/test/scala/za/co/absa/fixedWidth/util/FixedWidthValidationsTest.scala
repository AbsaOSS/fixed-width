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

import org.scalatest.FunSuite
import za.co.absa.fixedWidth.ValidationsException

class FixedWidthValidationsTest extends FunSuite {

  test("ValidateRead - positive") {
    val params = Map(
      "path" -> "/alfa/beta",
      "trimValues" ->  "true",
      "treatEmptyValuesAsNulls" -> "false",
      "charset" -> "UTF-16")
    FixedWidthValidations.validateRead(params)
  }

  test("ValidateRead - missing path") {
    val expectedMsg =
      """Parameter validations issues found. Issues are:
        |Path to source either empty or not defined""".stripMargin
    val msg = intercept[ValidationsException] { FixedWidthValidations.validateRead(Map("Something" ->  "else")) }
    assert(expectedMsg == msg.getMessage)
  }

  test("ValidateRead - bad value in trimValues") {
    val expectedMsg =
      """Parameter validations issues found. Issues are:
        |Unable to parse trimValues option. It should be only true or false. Got Some(blabla)""".stripMargin
    val msg = intercept[ValidationsException] {
      FixedWidthValidations.validateRead(Map("path" -> "/alfa/beta", "trimValues" -> "blabla"))
    }
    assert(expectedMsg == msg.getMessage)
  }

  test("ValidateRead - unsupported charset") {
    val charset = "ALFA"
    val expectedMsg =
      s"""Parameter validations issues found. Issues are:
         |Unable to parse charset option. $charset is invalid""".stripMargin
    val msg = intercept[ValidationsException] {
      FixedWidthValidations.validateRead(Map("path" -> "/alfa/beta", "charset" -> charset))
    }
    assert(expectedMsg == msg.getMessage)
  }

  test("ValidateWrite - positive") {
    FixedWidthValidations.validateWrite(Map("path" -> "/alfa/beta", "Something" ->  "else"))
  }

  test("ValidateWrite - missing path") {
    val expectedMsg =
      """Parameter validations issues found. Issues are:
        |Path to source either empty or not defined""".stripMargin
    val msg = intercept[ValidationsException] { FixedWidthValidations.validateWrite(Map("Something" ->  "else")) }
    assert(expectedMsg == msg.getMessage)
  }

}
