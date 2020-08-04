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
