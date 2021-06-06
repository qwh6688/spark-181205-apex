package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.PhoenixUtils;
import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class SaveToPhoenix {
    public static void main(String[] args) throws IOException {
        SparkConfig.initStatic();
        Dataset<Row> sparkJjjzEtf = SparkRuntime.registerTempView("SELECT  * from adp_bas.info_this_jjjz_etf WHERE rq = '20210419' LIMIT 1", "sparkJjjzEtf");
        PhoenixUtils.init();
        PhoenixUtils.saveToPhoenix(sparkJjjzEtf,"APEX","INFO_THIS_JJJZ_ETF");
        PhoenixUtils.close();
    }

}
