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
 * @date Created on 2021/6/2
 */
public class CollectAndCollectAsList {
        public static void main(String[] args) {
            SparkConfig.initStatic();
            Dataset<Row> sparkJjjzEtf = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' LIMIT 10", "sparkJjjzEtf");
//        collect()	在驱动程序中，以数组的形式返回数据集的所有元素
            List<Row> rows = sparkJjjzEtf.collectAsList();
//            Row[] collect = sparkJjjzEtf.collect();
            //命令行执行
        /*[
            [20210419,SH,510010,1,1.4950,190520000.00,0.00,283000000.00,1.0840,10.3100,0.0171,1.4941,1.4944,0.0009,0.000000,0.000,0.0000,0.0000,0.00,0.00,1.4690,0.0177,0.000000,0.000000,0.00,20210419],
            [20210419,SH,510010,1,1.4950,190520000.00,0.00,283000000.00,1.0840,10.3100,0.0171,1.4941,1.4944,0.0009,0.000000,0.000,0.0000,0.0000,0.00,0.00,1.4690,0.0177,0.000000,0.000000,0.00,20210419]
        ]
        */
            sparkJjjzEtf.show();
//        demo01(SparkConfig.getSparkSession());
//      sparkJjjzEtf.collect();


        }

}

