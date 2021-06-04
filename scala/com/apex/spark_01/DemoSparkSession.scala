package com.apex.spark_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description:
  *
  * @author quwh
  * @date Created on 2021/6/4
  */
object DemoSparkSession {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("DemoSparkSession")
      .master("local[1]")
      .getOrCreate
    val sc = spark.sparkContext
    //    val lineRDD: RDD[Array[String]] = sc.textFile("file:///D:/in/people.txt").map(_.split(" "))
    val lineRDD: RDD[Array[String]] = sc.textFile("file:///D:/in/people.txt").map(_.split(" "))

    //    lineRDD.collect().foreach(_.foreach(println(_)))
    val personRDD: RDD[Person] = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF
    personDF.printSchema()
    personDF.show()

  }


}
