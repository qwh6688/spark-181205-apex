/*

package com.apex.spark_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:
  *
  * @author quwh
  * @date Created on 2021/6/4
  */
object RDD {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(config)
//
//    val spark: SparkSession = DemoSparkSession.builder()
//      .appName("RDD")
//      .master("local[*]").getOrCreate()
    //    从本地读取文件
    val lineRDD: RDD[Array[String]] = sc.textFile("file:///D:/in/people.txt").map(_.split(" "))
    val personRDD: RDD[Person] = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF
    personDF.printSchema()

    personDF.show


    val array: Array[Array[String]] = lineRDD.collect()
    array.foreach(_.foreach(println(_)))
    sc.stop()
  }

}

*/
