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

class ParseModesTest extends FunSuite {

  test("IsFailFastMode - true") {
    assert(ParseModes.isFailFastMode("FAILFAST"))
  }

  test("IsFailFastMode - false") {
    assert(!ParseModes.isFailFastMode("don't"))
  }

  test("IsValidMode - true") {
    assert(ParseModes.isValidMode("PERMISSIVE"))
  }

  test("IsValidMode - false") {
    assert(!ParseModes.isValidMode("don't"))
  }

  test("IsPermissiveMode - true") {
    assert(ParseModes.isPermissiveMode("PERMISSIVE"))
  }

  test("IsPermissiveMode - true - nonValid mode defaults to true") {
    assert(ParseModes.isPermissiveMode("don't"))
  }

  test("IsPermissiveMode - false") {
    assert(!ParseModes.isPermissiveMode("DROPMALFORMED"))
  }

  test("IsDropMalformedMode - true") {
    assert(ParseModes.isDropMalformedMode("DROPMALFORMED"))
  }

  test("IsDropMalformedMode - false") {
    assert(!ParseModes.isDropMalformedMode("don't"))
  }

}
