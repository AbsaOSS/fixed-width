# Fixed-Width Data Source for Apache Spark

A library for parsing FixedWidth data with Apache Spark. FixedWidth file is a flat file where each column has a fixed width (number of characters) and this is specified in a 
schema.

## Usage

You can link against this library in your program at the following coordinates:

#### Scala 2.11

```
groupId: za.co.absa
artifactId: fixed-width_2.11
version: 0.2.0
```

#### Scala 2.12

```
groupId: za.co.absa
artifactId: fixed-width_2.12
version: 0.2.0
```
 

### Using with Spark shell
This package can be added to Spark using the `--packages` command line option. For example, to include it when starting the spark shell:


#### Spark compiled with Scala 2.11

```shell
$SPARK_HOME/bin/spark-shell --packages za.co.absa:fixed-width_2.11:0.2.0
```

#### Spark compiled with Scala 2.12

```shell
$SPARK_HOME/bin/spark-shell --packages za.co.absa:fixed-width_2.12:0.2.0
```

### Usage in code

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

val sparkBuilder = SparkSession.builder().appName("Example")
val spark = sparkBuilder.getOrCreate()

val metadata = new MetadataBuilder().putLong("width", 10).build()
val schema = StructType(
  List(
    StructField("someColumn", StringType, true, metadata),
    StructField("secondColumn", StringType, true, metadata)
  )
)

val dataframe = spark
  .read
  .format("fixed-width")
  .option("trimValues", "true")
  .schema(schema)
  .load("/path/to/data/fixedWidthData")
```

### Options
| Option | Value | Explanation |
|---|---|---|
| trimValues | true/false | Should the whitespaces around data be trimmed |
| charset | charset name (e.g. UTF-8) | Any valid charset used to write the FixedWidth file |

### How to generate Code coverage report
```sbt
sbt jacoco
```
Code coverage will be generated on path:
```
{project-root}/target/scala-{scala_version}/jacoco/report/html
```
