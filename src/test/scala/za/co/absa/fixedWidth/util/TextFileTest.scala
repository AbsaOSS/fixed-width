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

import java.nio.charset.UnsupportedCharsetException

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class TextFileTest extends FunSuite with SparkSessionTestWrapper {
  private val expectedText =
    """A guy asks a girl to go to a dance. She agrees, and he decides to rent a suit. The rental has a long line, so he waits and waits, and finally he gets his suit.
      |He decides to buy flowers, so he goes to the flower shop. The flower shop has a long line, so he waits and waits, until he finally buys flowers.
      |He picks up the girl and they go to the dance. There is a long line into the dance, so they wait and wait.
      |Finally, they get into the dance, and the guy offers to get the girl a drink. She asks for punch, so he goes to the drink table, and there is no punch line.""".stripMargin

  test("WithCharset - default charset") {
    val rss = getClass.getResource("/testTextFile.txt").getPath
    val rdd: RDD[String] = TextFile.withCharset(spark.sparkContext, rss, "UTF-8")
    assert(expectedText == rdd.collect().mkString("\n"))
  }

  test("WithCharset - custom charset") {
    val rss = getClass.getResource("/testTextFile.txt").getPath
    val rdd: RDD[String] = TextFile.withCharset(spark.sparkContext, rss, "ASCII")
    assert(expectedText == rdd.collect().mkString("\n"))
  }

  test("WithCharset - bad charset") {
    val expectedMsg = "alfaOmega"
    val msg = intercept[UnsupportedCharsetException] {
      TextFile.withCharset(spark.sparkContext, "location", "alfaOmega")
    }
    assert(expectedMsg == msg.getMessage)
  }

  test("WithCharset - bad location") {
    val expectedMsg = "Input path does not exist: file:/bogusLocation.txt"
    val msg = intercept[InvalidInputException] {
      val rdd = TextFile.withCharset(spark.sparkContext, "/bogusLocation.txt", "UTF-8")
      rdd.collect()
    }
    assert(expectedMsg == msg.getMessage)
  }

}
