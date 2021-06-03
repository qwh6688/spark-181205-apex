package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/2
 */
public class UnionALLRegisterTempView {
    public static void main(String[] args) {
        SparkConfig.initStatic();
        Dataset<Row> sparkJjjzEtf_01 = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' AND `jys` ='SH' AND `jjdm` ='510010' LIMIT 1 ", "sparkJjjzEtf1");
        Dataset<Row> sparkJjjzEtf_02 = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' AND `jys` ='SH' AND `jjdm` ='510010' LIMIT 1 ", "sparkJjjzEtf1");
        SparkRuntime.execUnionALLRegisterTempView(Arrays.asList(sparkJjjzEtf_01,sparkJjjzEtf_02),"unionTemp");
//        SparkConfig.getSparkSession().sql("select * from unionTemp ").show();
/*
|      rq|jys|  jjdm|jjlx|  jjjz|        jjfe|xzfe|        jjgm|  zspb|   zspe|  zszd|  gsjz|gsjz_jsl|  gzpc|     yjl|  zxj|zxj_zdf|zxj_zde|cjsl|cjje|jjjz_sr|jjjz_zdf|    fhps|     hsl|drsp|    jzrq|
|20210419| SH|510010|   1|1.4950|190520000.00|0.00|283000000.00|1.0840|10.3100|0.0171|1.4941|  1.4944|0.0009|0.000000|0.000| 0.0000| 0.0000|0.00|0.00| 1.4690|  0.0177|0.000000|0.000000|0.00|20210419|
|20210419| SH|510010|   1|1.4950|190520000.00|0.00|283000000.00|1.0840|10.3100|0.0171|1.4941|  1.4944|0.0009|0.000000|0.000| 0.0000| 0.0000|0.00|0.00| 1.4690|  0.0177|0.000000|0.000000|0.00|20210419|
*/
        SparkConfig.getSparkSession().sql("select * from unionTemp ").distinct().show();
     }
}
