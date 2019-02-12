package com.mappy
import com.mappy.LogRunner._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, SequentialNestedSuiteExecution}


class LogTest extends FunSuite with SequentialNestedSuiteExecution with SparkSessionTestWrapper  {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val logFile = getClass.getResource("/tornik-map-20171006.10000.tsv").getPath

  val df = spark.read.textFile(logFile).toDF("value")
  val pattern = "/map/1.0/slab/.*/.*/.*/.*/"

  test("Test 0: Read Raw File ") {
    df.show
    println(df.count)

  }

  test("Test 1 : Filtred DF") {
    val filtredDf= df.transform(filterWithRegex(col("value"),pattern))
    println(filtredDf.count)
  }

  test("Test 2: Split DF into multiple Columns") {
    val splittedDf =  df.transform(filterWithRegex(col("value"),pattern))
      .transform(withColumnsFromSplit())
    splittedDf.show()
    println(splittedDf.count)
  }

  test("Test 3: Mode View with count") {
    val countDf= df.transform(filterWithRegex(col("value"),pattern))
      .transform(withColumnsFromSplit())
      .transform(getCountiewModes)
    countDf.show()
    println(countDf.count)
  }

  test("Test 4: Mode View with count and Zoom values") {
    val countDfWithZoom = df.transform(filterWithRegex(col("value"),pattern))
      .transform(withColumnsFromSplit())
      .transform(getCountiewModesWithZoom)
    countDfWithZoom.show()
    println(countDfWithZoom.count)
  }

}
