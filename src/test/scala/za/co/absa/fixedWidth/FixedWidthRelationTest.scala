package za.co.absa.fixedWidth

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.fixedWidth.util.SparkSessionTestWrapper

class FixedWidthRelationTest extends FunSuite with SparkSessionTestWrapper {
  private val filePath = getClass.getResource("/fixed-width.txt").getPath

  private val width20Metadata = new MetadataBuilder()
    .putString("width", "20")
    .build()

  private val width3Metadata = new MetadataBuilder()
    .putLong("width", 3L)
    .build()

  private val defaultSchema = StructType(
    List(
      StructField("id", IntegerType, nullable = true, width3Metadata),
      StructField("Name", StringType, nullable = true, width20Metadata)
    )
  )

  private val defaultSchemaStrings = StructType(
    List(
      StructField("id", StringType, nullable = true, width3Metadata),
      StructField("Name", StringType, nullable = true, width20Metadata)
    )
  )

  test("Read FixedWidth all string") {
    val result = spark
      .read
      .format("fixed-width")
      .schema(defaultSchemaStrings)
      .load(filePath)

    assert(4 == result.collect().length)
    assert(defaultSchemaStrings == result.schema)
  }

  test("Read FixedWidth natural types") {
    val result = spark
      .read
      .format("fixed-width")
      .option("trimValues", "true")
      .schema(defaultSchema)
      .load(filePath)

    assert(4 == result.collect().length)
    assert(defaultSchema == result.schema)
  }
}
