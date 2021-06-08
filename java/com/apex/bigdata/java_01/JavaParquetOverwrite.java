package com.apex.bigdata.java_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.SQLContext;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/6
 */
public class JavaParquetOverwrite {
    public static void main(String[] args) {

        SparkConfig.initStatic();
        SparkConfig.getSparkSession().sql("select * from myhive.parquet_test1").show();
        SparkRuntime.exec("desc adp_bas.info_this_jjjz_etf").show();
        SparkRuntime.exec("select * from myhive.parquet_test1").show(10);
//        SparkConfig.getSparkSession().sql("insert overwrite  table myhive.parquet_test2 select id , name from myhive.parquet_test1");
        System.out.println("执行成功");


    }
}
