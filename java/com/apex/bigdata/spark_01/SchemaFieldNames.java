package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/3
 */
public class SchemaFieldNames {
    public static void main(String[] args) {
        SparkConfig.initStatic();
        Dataset<Row> sparkJjjzEtf = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' LIMIT 10", "sparkJjjzEtf");
//        String[] fileds = exec("select * from " + viewName).schema().fieldNames();
        String[] fieldNames = SparkRuntime.exec("select * from sparkJjjzEtf ").schema().fieldNames();
        //fori
        for (int i = 0; i < fieldNames.length; i++) {
            System.out.println(fieldNames[i]);

        }

    }
}
