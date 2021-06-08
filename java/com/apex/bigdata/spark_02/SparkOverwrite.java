package com.apex.bigdata.spark_02;

import com.apex.bigdata.template.SparkConfig;
import org.tukaani.xz.simple.SPARC;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/6
 */
public class SparkOverwrite {
    public static void main(String[] args) {
        SparkConfig.initStatic();
        SparkConfig.getSparkSession().sql("select * from myhive.kwang_test").createOrReplaceTempView("sparkKwangTest");
        SparkConfig.getSparkSession().sql("select * from sparkKwangTest").show();
        SparkConfig.getSparkSession().sql("insert overwrite  table myhive.kwang_test_2 select id , name from sparkKwangTest");
        System.out.println("执行成功");


    }
}
