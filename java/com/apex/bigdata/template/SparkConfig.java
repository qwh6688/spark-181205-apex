package com.apex.bigdata.template;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Admin on 2018/3/22.
 * Spark 环境初始化类，用于初始化Spark环境
 * <p>
 * 环境有默认配置属性和spark调优参数，用户也可以自定义设置配置文件。
 * 通过getInstance.setPath() 方法传入配置文件即可自定义配置 路径为任意绝对路径
 * 支持从Mysql中读取数据
 * 支持从Orcle中读取数据(todo)
 * 支持hive 互操作
 */
public class SparkConfig {
    public static SparkSession spark;
    //序列化数组类
    private static Class[] classes = {};

    public static void setClasses(Class[] classList) {
        classes = classList;
    }

    public static SparkSession getSparkSession() {
        return spark;
    }

    public static JavaSparkContext javaSparkContext;
    private static List<String> sparkConfs = Arrays.asList(
            "spark.files.fetchTimeout",
            "spark.serializer",
            "spark.scheduler.mode",
            "spark.default.parallelism",
            "spark.yarn.executor.memoryOverhead",
            "spark.debug.maxToStringFields",
            "spark.executor.extraJavaOptions",
            "spark.sql.parquet.writeLegacyFormat",
            "spark.sql.warehouse.dir",
            "hive.metastore.uris",
            "spark.sql.shuffle.partitions",
            "spark.sql.parquet.cacheMatadata",
            "spark.sql.autoBroadcastJoinThreshold",
            "spark.shuffle.memoryFraction",
            "spark.storage.memoryFraction",
            "spark.executor.momory",
            "spark.driver.memory",
            "spark.driver.extraJavaOptions",
            "spark.sql.codegen,aggregate.map.twolevel.enable",
            "spark.sql.parquet.compression.codec",
            "spark.executor.heartbeatInterval",
            "spark.sql.hive.metastorePartitionPruning",
            "spark.sql.crossJoin.enabled"
    );

    public static void init() {
        SparkConf sparkConf = new SparkConf();
        if (PropertyUtils.confs.containsKey("Master")) {
            sparkConf.setMaster(PropertyUtils.confs.get("Master"));
        }
        if (classes.length > 0) {
            sparkConf.registerKryoClasses(classes);
        }
        System.out.println("当前任务计算客户数：" + PropertyUtils.getCusts());
        if (PropertyUtils.getCusts() > 0) {
            if (PropertyUtils.getCusts() <= 5) {
                sparkConf.set("spark.default.parallelism", "20");
                sparkConf.set("spark.sql.shuffle.partitions", "30");
            } else {
                sparkConf.set("spark.default.parallelism", "50");
                sparkConf.set("spark.sql.shuffle.partitions", "100");
            }
        } else {
            for (String conf : sparkConfs) {
                if (PropertyUtils.confs.containsKey(conf)) {
                    sparkConf.set(conf, PropertyUtils.confs.get(conf));
                }
            }
        }

        SparkSession.Builder builder = SparkSession.builder();
        builder.config(sparkConf);
        builder.enableHiveSupport();
        spark = builder.getOrCreate();
        javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        javaSparkContext.setLocalProperty("spark.scheduler.pool", null);
    }

    public static void initStatic() {
        String WarehourDir = "hdfs://node01:8020/user/hive/warehouse";
        //String HiveMetastoreUris = "thrift://cdh02.ebscncdh.com:9083";;
        String HiveMetastoreUris = "thrift://node01:9083";

        //本地 local[*]
        //制定 spark://192.168.0.87:7077
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("iDS-Spark::IdsRunner"); //spark://192.168.0.87:7077  local[*]
        if (classes.length > 0) {
            sparkConf.registerKryoClasses(classes);
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        sparkConf.set("spark.default.parallelism", "30");
        sparkConf.set("spark.memory.useLegacyMode", "true");

        sparkConf.set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=256M");
        sparkConf.set("spark.debug.maxToStringFields", "200");
        sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1");

        sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true");
        sparkConf.set("spark.files.fetchTimeout", "9000");
//        sparkConf.set("spark.sql.shuffle.partitions", "50");

        sparkConf.set("spark.sql.parquet.cacheMatadata", "true");
        sparkConf.set("spark.yarn.executor.memoryOverhead", "8192");
        SparkSession.Builder builder = SparkSession.builder();
        builder.config(sparkConf);
        builder.config("spark.sql.warehouse.dir", WarehourDir);
        builder.enableHiveSupport();
        spark = builder.getOrCreate();
        javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        javaSparkContext.setLocalProperty("spark.scheduler.pool", null);
    }
}
