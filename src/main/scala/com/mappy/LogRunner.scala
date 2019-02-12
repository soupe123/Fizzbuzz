package com.mappy

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LogRunner {

  /** Usage: log [file] */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Logfile <file>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("Mappy Log Exercice")
      .getOrCreate()


    val logFile = getClass.getResource(args(0)).getPath
    val df = spark.read.textFile(logFile).toDF("value")
    val pattern = "/map/1.0/slab/.*/.*/.*/.*/"

    val countDfWithZoom = df.transform(filterWithRegex(col("value"), pattern))
      .transform(withColumnsFromSplit())
      .transform(getCountiewModesWithZoom)


    countDfWithZoom.show()
    println(countDfWithZoom.count)

    spark.stop()

  }



  //Filter unformatted data
  def filterWithRegex(column: Column, pattern: String)(df:DataFrame): DataFrame = {
    df.filter(column.rlike(pattern))
  }


  //Split a column value into multiple columns
  def withColumnsFromSplit()(df: DataFrame): DataFrame = {
    val headerLength = df.rdd.first.toString.split("/").length

    df.withColumn("value" , split(col("value"), "/"))
      .select((0 until headerLength)
        .map(i=>col("value")
          .getItem(i).as("column_"+ i))
        :_* )
  }


  //Count distinct viewmodes
  def getCountiewModes(df: DataFrame) : DataFrame ={
    df.groupBy("column_4").count()
  }


  //Count distinct viewmodes and collect all the values of columns_6
  def getCountiewModesWithZoom(df: DataFrame) : DataFrame ={
    df.withColumnRenamed("column_4","ViewMode")
      .groupBy("ViewMode")
      .agg(count("ViewMode").as("CountViewModes"),concat_ws(",",
        collect_set("column_6")).as("Zoom"))
  }






}
