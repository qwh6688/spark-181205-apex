package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/2
 */
public class CreateHiveTempTable {
    public static void main(String[] args) {
        SparkConfig.initStatic();
        Dataset<Row> sparkJjjzEtf = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' LIMIT 10", "sparkJjjzEtf");
        SparkRuntime.createHiveTempTable("sparkJjjzEtf", "adp_usr", "ids_tmp_sparkJjjzEtf");
        SparkConfig.getSparkSession().catalog().refreshTable("adp_usr" + "." + "ids_tmp_sparkJjjzEtf");

        SparkConfig.getSparkSession().sql("select * from adp_usr.ids_tmp_sparkJjjzEtf").show();


    }
}
