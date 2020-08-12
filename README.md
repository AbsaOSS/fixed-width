# Fixed-Width Data Source for Apache Spark

A library for parsing FixedWidth data with Apache Spark. FixedWidth file is a flat file where each column has a fixed width (number of characters) and this is specified in a 
schema.

## Usage

You can link against this library in your program at the following coordinates:

#### Scala 2.11

```
groupId: za.co.absa.cobrix
artifactId: spark-cobol_2.11
version: 0.1.0
```

#### Scala 2.12

Can be compiled and published on request. Project should be compatible.
 

### Using with Spark shell
This package can be added to Spark using the `--packages` command line option. For example, to include it when starting the spark shell:


#### Spark compiled with Scala 2.11

```shell
$SPARK_HOME/bin/spark-shell --packages za.co.absa:fixed-width.11:0.1.0
```

### Usage in code

```scala
val sparkBuilder = SparkSession.builder().appName("Example")
val spark = sparkBuilder.getOrCreate()

val metadata = new MetadataBuilder().putLong("width", 10).build()
val schema = StructType(
  List(
    StructField("someColumn", StringType, metadata),
    StructField("secondColumn", StringType, metadata)
  )
)

val dataframe = spark
  .read
  .format("fixed-width")
  .option("trimValues", "true")
  .schema(shema)
  .load("/path/to/data/fixedWidthData")
```

### Options
| Option | Value | Explanation |
|---|---|---|
| trimValues | true/false | Should the whitespaces around data be trimmed |
| charset | charset name (e.g. UTF-8) | Any valid charset used to write the FixedWidth file |