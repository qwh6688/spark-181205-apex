package com.apex.spark_01

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * Description:
  *
  * @author quwh
  * @date Created on 2021/6/7
  */
object ParquetOverwrite {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("hive example")
      .config("spark.sql.warehouse.dir", "hdfs://node01:8020/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://node01:9083")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("select *  from myhive.info_this_jjjz_etf").show();
    println("执行成功")
    //    rowDataset.write.mode(SaveMode.Overwrite).saveAsTable("myhive.test1")

  }

}
