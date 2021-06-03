package com.apex.spark_01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:
  *
  * @author quwh
  * @date Created on 2021/6/2
  */
object Union {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Union")
    val sc: SparkContext = new SparkContext(config)
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))
    //求并集
    val rdd3 = rdd1.union(rdd2)
  // Array(5, 6, 4, 3, 1, 2, 3, 4)   不带去重效果
    rdd3.collect().foreach(println)
    //求交集
    val rdd4 = rdd1.intersection(rdd2)
    rdd4.collect().foreach(println)
  /*  //去重
    rdd3.distinct.collect*/
    rdd4.collect


  }
}
