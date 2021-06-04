package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/1
 */
public class RegisterTempView {
    public static void main(String[] args) {
        SparkConfig.initStatic();
        Dataset<Row> sparkJjjzEtf = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' LIMIT 10", "sparkJjjzEtf");
//        collect()	在驱动程序中，以数组的形式返回数据集的所有元素
        //命令行执行
        List<Row> rows = sparkJjjzEtf.collectAsList();
//        Row[] collect = sparkJjjzEtf.collect();

        sparkJjjzEtf.show();
//        demo01(SparkConfig.getSparkSession());
//      sparkJjjzEtf.collect();

        Dataset<Row> sparkJjjzEtf1 = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' LIMIT 10", "sparkJjjzEtf");
    }

    public static void demo01(SparkSession sparkSession) {
        System.out.println("执行聚合运算");
        sparkSession.sql("select count(1) from sparkJjjzEtf").show();
    }

}
