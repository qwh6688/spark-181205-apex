package com.apex.bigdata.spark_01;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

       /* rq	int
        jys	string
        jjdm	string
        jjlx	decimal(12,0)
        jjjz	decimal(10,4)
        jjfe	decimal(16,2)
        xzfe	decimal(16,2)
        jjgm	decimal(16,2)
        zspb	decimal(10,4)
        zspe	decimal(10,4)
        zszd	decimal(9,4)
        gsjz	decimal(10,4)
        gsjz_jsl	decimal(10,4)
        gzpc	decimal(9,4)
        yjl	decimal(9,6)
        zxj	decimal(9,3)
        zxj_zdf	decimal(9,4)
        zxj_zde	decimal(9,4)
        cjsl	decimal(16,2)
        cjje	decimal(16,2)
        jjjz_sr	decimal(10,4)
        jjjz_zdf	decimal(9,4)
        fhps	decimal(10,6)
        hsl	decimal(9,6)
        drsp	decimal(9,2)
        jzrq	int
        # Partition Information
        # col_name	data_type
        jzrq	int*/

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/2
 */
public class CacheHiveTableSchema {
    private static Logger logger = LoggerFactory.getLogger(CacheHiveTableSchema.class);

    public static Map<String, Tuple2<List<String>, List<String>>> cacheTableSchema = new HashMap<>(); //缓存使用过的元数据
    public static Map<String, String> cachePartitionColumn = new HashMap<>(); //缓存使用过的表分区字段

    public static void main(String[] args) {
        SparkConfig.initStatic();
        cacheHiveTableSchema("adp_cfg", "info_this_jjjz_etf");

    }

    //执行Spark语句
    public static Dataset<Row> exec(String sqlText) {
        try {
            return SparkConfig.getSparkSession().sql(sqlText);
        } catch (Exception e) {
            logger.info(sqlText);
            logger.error("Running Error : ", e.fillInStackTrace());
        }
        return null;
    }

    public static void cacheHiveTableSchema(String dbname, String tablename) {
        // 将hive的schema缓存起来
        List<String> columnName = new ArrayList<>(); //字段名
        List<String> columnType = new ArrayList<>(); //字段类型
        Boolean isFields = true;
        if (cacheTableSchema.containsKey(dbname + "." + tablename)) { //缓存中是否已有
            String tbSchema = dbname + "." + tablename;
            System.out.println(tbSchema + " 表结构已存在");
        } else {
            try {
                List<Row> descList = exec("desc " + dbname + "." + tablename).collectAsList();
                for (Row row : descList) {
                    // System.out.println(row.get(0)+"\t"+row.get(1)+"\t"+row.get(2));
                    if (row.getAs("col_name").toString().startsWith("# ")) {
                        isFields = false;
                    } else if (!row.getAs("col_name").toString().startsWith("# ") && isFields == true) {
                        columnName.add(row.getAs("col_name").toString());
                        columnType.add(row.getAs("data_type").toString());
                    } else {
                        cachePartitionColumn.put(dbname + "." + tablename, row.getAs("col_name").toString());
                    }

                }
                if (columnName.size() != 0) {
                    cacheTableSchema.put(dbname + "." + tablename, new Tuple2<>(columnName, columnType));
                }
            } catch (Exception e) {
                logger.info("cacheHiveTableSchema table:" + dbname + "." + tablename + " failed!");
                logger.error("Running Error : ", e.fillInStackTrace());
                System.exit(1);
            }


        }


    }
}
