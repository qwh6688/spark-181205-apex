package com.apex.spark_01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:
  *
  * @author quwh
  * @date Created on 2021/4/17
  */
//伴生对象
object WordCount {
  //两种方法启动程序
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(config)
    val wordCount: Array[(String, Int)] = sc.textFile("in").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()
    wordCount.foreach(println)
    sc.stop()
//    println(wordCount)  Lscala.Tuple2;@4cdb8504

  }

}
