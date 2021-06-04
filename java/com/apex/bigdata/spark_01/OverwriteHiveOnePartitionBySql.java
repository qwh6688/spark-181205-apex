package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static com.apex.bigdata.template.SparkRuntime.*;


/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/3
 */
public class OverwriteHiveOnePartitionBySql {
    private static Logger logger = LoggerFactory.getLogger(OverwriteHiveOnePartitionBySql.class);


    public static void main(String[] args) {
        SparkConfig.initStatic();
        Dataset<Row> sparkJjjzEtf = SparkRuntime.registerTempView("SELECT  * from adp_cfg.`info_this_jjjz_etf` WHERE rq = '20210419' LIMIT 10", "sparkJjjzEtf");
//        SparkConfig.getSparkSession().sql("select * from adp_bas.info_this_jjjz_etf").show();
        overwriteHiveOnePartitionBySql("sparkJjjzEtf", "adp_bas", "info_this_jjjz_etf", 20210419);


    }


    public static void overwriteHiveOnePartitionBySql(String viewName, String dbName, String targetTableName, Integer rq) {
        try {
            /*
        将spark临时表与hive表数据类型进行对应初始化
         */
            cacheHiveTableSchema(dbName, targetTableName);
            List<String> columnName = new ArrayList<>(); //字段名
            List<String> columnType = new ArrayList<>(); //字段类型
            String partitionColumn = cachePartitionColumn.get(dbName + "." + targetTableName);
            if (cacheTableSchema.containsKey(dbName + "." + targetTableName)) {
                columnName = cacheTableSchema.get(dbName + "." + targetTableName)._1();
                columnType = cacheTableSchema.get(dbName + "." + targetTableName)._2();
                StringBuffer sql = new StringBuffer(String.format("insert overwrite table %s.%s partition(%s=%d) select ",
                        dbName,
                        targetTableName,
                        partitionColumn,
                        rq));
                try {
                    String[] fileds = exec("select * from " + viewName).schema().fieldNames();
                    Set<String> fieldSet = new HashSet<>();
                    for (int i = 0; i < fileds.length; i++) {
                        fieldSet.add(fileds[i].toLowerCase());
                    }
                    for (int i = 0; i < columnName.size(); i++) {
                        if (!partitionColumn.toLowerCase().equals(columnName.get(i).toLowerCase())) {
                            if (fieldSet.contains(columnName.get(i).toLowerCase())) {
                                sql.append("cast(" + columnName.get(i) + " as " + columnType.get(i) + ") as " + columnName.get(i));
                            } else {
                                sql.append("cast(null as " + columnType.get(i) + ") as " + columnName.get(i));
                            }
                        }
                        if (i < columnName.size() - 2) {
                            sql.append(",");
                        }
                    }
                    sql.append(" from " + viewName);//重新注册spark临时表
                    System.out.println("执行的sql语句是： " + sql.toString());
                    exec(sql.toString());
                } catch (Exception e) {
                    logger.info("Use spark tempView " + viewName + " initSparkResultTempView table:" + dbName + "." + targetTableName);
                    logger.error("Running Error : ", e.fillInStackTrace());
                    System.exit(1);
                }
            } else {
                logger.info("No table named: " + dbName + "." + targetTableName);
            }
        } catch (Exception e) {
            logger.info("Use spark tempView " + viewName + " overwriteHivePartition table:" + dbName + "." + targetTableName);
            logger.error("Running Error : ", e.fillInStackTrace());
            System.exit(1);
        }
    }
}
