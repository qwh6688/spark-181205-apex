package com.apex.bigdata.template;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/1
 */
public class SparkRuntime {
    //注册日志
    private static Logger logger = LoggerFactory.getLogger(SparkRuntime.class);
    private static List<Dataset<Row>> persistList = new ArrayList<>();
    //    private static List<Broadcast> broadcastList = new ArrayList<>();
//    public static List<Dataset<Row>> procCacheData = new ArrayList<>();
    public static List<String> tempTableList = new ArrayList<>();
    private static String mode = null;
    private static String date = new SimpleDateFormat("yyyyMMdd").format(new Date(System.currentTimeMillis()));

    public static boolean isDebug() {
        if (mode == null) {
            if (PropertyUtils.arguments.containsKey("MODE")) {
                System.out.println("--------存在MODE参数-------");
                mode = PropertyUtils.arguments.get("MODE");
                System.out.println("--------MODE值：" + mode);
            } else {
                mode = "normal";
            }
        }
        if (mode.toLowerCase().equals("debug")) {
            System.out.println("------当前模式Debug-----");
            return true;
        } else {
            return false;
        }
    }


    public static Dataset<Row> debugCreateTempTable(String sqlText, String tablename) {
        try {
            SparkConfig.getSparkSession().sql(sqlText).createOrReplaceTempView(tablename);
            SparkConfig.getSparkSession().sql(String.format("drop table if exists temp.%s", tablename));
            SparkConfig.getSparkSession().sql(String.format("create table temp.%s as select * from %s", tablename, tablename));
            Dataset<Row> ds = SparkConfig.getSparkSession().sql("select * from temp." + tablename);
            return ds;
        } catch (Exception e) {
            logger.info(sqlText);
            logger.error("Running Error : ", e.fillInStackTrace());
            System.exit(1);
        }
        return null;
    }


    //注册Spark临时视图`
    public static Dataset<Row> registerTempView(String sqlText, String tablename) {
        try {
            Dataset<Row> ds = exec(sqlText, tablename);
            ds.createOrReplaceTempView(tablename);
            return ds;
        } catch (Exception e) {
            logger.info(sqlText);
            logger.error("Running Error : ", e.fillInStackTrace());
            System.exit(1);
        }
        return null;
    }

    public static Dataset<Row> execUnionALLRegisterTempView(List<Dataset<Row>> datasets, String tablename) {
        try {
            Dataset<Row> ds = datasets.get(0);
            for (int i = 1; i < datasets.size(); i++) {
                try {
                    ds = ds.union(datasets.get(i));
                } catch (Exception e) {
                }
            }
            ds.createOrReplaceTempView(tablename);
//            persistList.add(ds);
            return ds;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Running Error : ", e.fillInStackTrace());
            System.exit(1);
        }
        return null;
    }

    public static Dataset<Row> exec(String sqlText, String tablename) {
        try {
            if (isDebug()) {
                System.out.println("开始创建临时视图" + tablename);
                long t1 = System.currentTimeMillis();
                Dataset<Row> ds = debugCreateTempTable(sqlText, tablename);
                long t2 = System.currentTimeMillis();
                System.out.println("创建临时视图" + tablename + "共耗时：" + (t2 - t1) / 1000 + "秒");
//                System.out.println("临时视图"+tablename+"数据量：" + ds.count());
                return ds;
            } else {
                Dataset<Row> ds = SparkConfig.getSparkSession().sql(sqlText);
                return ds;
            }
        } catch (Exception e) {
            logger.info(sqlText);
            logger.error("Running Error : ", e.fillInStackTrace());
            System.exit(1);
        }
        return null;
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

    public static void createHiveTempTable(String viewName, String dbName, String targetTableName) {
        /**
         * 创建hive临时表，确保不使用在生产表，且表名已TMP开头
         */
        String sqlText;
        try {
            if (targetTableName.toLowerCase().startsWith("tmp") || targetTableName.toLowerCase().startsWith("ids_tmp")) {
                exec("drop table if exists " + dbName + "." + targetTableName);
                sqlText = "create table " + dbName + "." + targetTableName +
                        "  as select * from " + viewName; //using hive options(fileFormat 'parquet')
                exec(sqlText);
                //refreshHiveTable(dbName, targetTableName);
                sqlText = "select * from " + dbName + "." + targetTableName;
                tempTableList.add(dbName + "." + targetTableName);
                SparkConfig.getSparkSession().sql(sqlText).createOrReplaceTempView(viewName);
//                registerTempView(sqlText, viewName);
            } else {
                logger.info("createHiveTempTable " + dbName + "." + targetTableName + " info error: table Name must start with tmp or ids_tmp");
                System.exit(1);
            }
            SparkConfig.getSparkSession().catalog().refreshTable(dbName + "." + targetTableName);
        } catch (Exception e) {
            logger.info("createHiveTempTable " + dbName + "." + targetTableName + " error: table Name must start with tmp or ids_tmp");
            logger.error("Running Error : ", e.fillInStackTrace());
            System.exit(1);
        }
    }


    public static Map<String, Tuple2<List<String>, List<String>>> cacheTableSchema = new HashMap<>(); //缓存使用过的元数据
    public static Map<String, String> cachePartitionColumn = new HashMap<>(); //缓存使用过的表分区字段

    public static void cacheHiveTableSchema(String dbname, String tablename) {
        //将hive的schema缓存起来
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
