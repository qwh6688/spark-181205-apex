package com.apex.bigdata.publicProcess.test;

import com.apex.bigdata.publicProcess.MoveFinfo;
import com.apex.bigdata.template.*;
import com.apex.bigdata.template.MyEnum.Conf;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.*;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/6
 */
public class DemoMoveFinfo {
    private static Logger logger = LoggerFactory.getLogger(DemoMoveFinfo.class);
    private static String DB = "adp_cfg";
    private static String MysqlDB = "info";
    private static String getJobSql = "select table_name,sync_type,sync_col,jyr_dif from pub_sys.t_sync_table_aliyun order by seq";
    private static Integer RQ;
    private static TradingDays tradingDays;
    private static Boolean tranCode = true;
    private static com.apex.bigdata.template.MysqlUtil MysqlUtil;
    private static final Set<String> marketField = Sets.newHashSet("jys", "tadm");
    private static final Set<String> currencyField = Sets.newHashSet("bz");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: IDS Spark <file>");
            System.exit(1);
        } else {
            //loadProperties 加载配置文件
            PropertyUtils.parseArgs(args);
        }
        try {
            //new MysqlUtil 初始化mysql连接配置
            MysqlUtil = new MysqlUtil(PropertyUtils.confs.getConfWithNoPrefix(Conf.mysql_bd));
            SparkConfig.initStatic();
            tradingDays = new TradingDays(SparkConfig.spark);
            if (PropertyUtils.arguments.containsKey("KSRQ")) {
                RQ = Integer.parseInt(PropertyUtils.arguments.get("KSRQ"));
            } else {
                logger.warn("Doesn't get conf:KSRQ");
                SparkConfig.spark.stop();
            }
            SparkRuntime.exec("set hive.exec.dynamic.partition.mode=nonstrict");
            String logId = PropertyUtils.arguments.get("LOGID");
            MysqlUtil.setLogId(logId);
            initDicUDF();
            ResultSet resultSet = MysqlUtil.executeSql(getJobSql);
//            table_name	sync_type	sync_col	jyr_dif
//            txggl	1		1                          1
            while (resultSet.next()) {
                Map<String, Object> job = new HashMap<>();
                job.put("table_name", resultSet.getObject("table_name"));
                job.put("sync_type", resultSet.getObject("sync_type"));
                job.put("sync_col", resultSet.getObject("sync_col"));
                job.put("jyr_dif", resultSet.getObject("jyr_dif"));
                MysqlUtil.logBegin("moveFinfo(" + job.get("table_name").toString().toUpperCase() + ")",
                        "moveFinfo(" + job.get("table_name").toString().toUpperCase() + ")",
                        RQ,
                        new Date(),
                        1,
                        "",
                        1,
                        null);
                try {
                    moveData(job);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                MysqlUtil.logEnd("moveFinfo(" + job.get("table_name").toString().toUpperCase() + ")",
                        "moveFinfo(" + job.get("table_name").toString().toUpperCase() + ")",
                        RQ,
                        new Date(),
                        2,
                        "",
                        1,
                        null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getLocalizedMessage());
            logger.error(logger.getName() + "  Running Error : " + e.fillInStackTrace());
            System.exit(1);
        }


    }

    /**
     * 迁移数据
     *
     * @param job 一张一张表同步
     *            table_name	sync_type	sync_col	jyr_dif
     *            txggl	1		1                       1
     */
    private static void moveData(Map<String, Object> job) {
        String table_name = job.get("table_name").toString();
        List<String> fileds = getFileds(table_name);
        System.out.println("mysql表 info." + table_name + " 的schema为：" + fileds.toString());
        StringBuffer sql = getSelectSql(job, fileds);
        System.out.println("查询sql为：" + sql);
        if (Integer.parseInt(job.get("sync_type").toString()) == 2) {
            int ksrq = tradingDays.getTradingDay(RQ, 0 - Integer.parseInt(job.get("jyr_dif").toString()));
            SparkRuntime.execReadFromMysql(sql.toString(), PropertyUtils.confs.getConfWithNoPrefix(Conf.mysql_bd)).createOrReplaceTempView("spark" + table_name);
            if (tranCode) {
                tranSparkTempViewCode(job, fileds, "spark" + table_name);
            }
            SparkRuntime.initSparkResultTempView("spark" + table_name, DB, "info_" + table_name);
            List<Integer> allday = tradingDays.getTradingDays(ksrq, RQ);
            for (int rq : allday) {
                long num = SparkRuntime.exec("select count(1) cn from " + "spark" + table_name + " where " + job.get("sync_col") + "=" + rq).collectAsList().get(0).getLong(0);
                if (num > 0) {
                    SparkRuntime.exec(getOverwriteSql(job, fileds, "spark" + table_name, rq).toString());
                }

            }
        } else if (Integer.parseInt(job.get("sync_type").toString()) == 1) {
            SparkRuntime.execReadFromMysql(sql.toString(), PropertyUtils.confs.getConfWithNoPrefix(Conf.mysql_bd)).createOrReplaceTempView("spark" + table_name);
            logger.info("第一次创建临时表： ");
            SparkConfig.getSparkSession().sql("select * from spark" + table_name + " limit 10").show();
            if (tranCode) {
                tranSparkTempViewCode(job, fileds, "spark" + table_name);
                logger.info("第二次创建临时表（转码后）： ");
                SparkConfig.getSparkSession().sql("select * from spark" + table_name + " limit 10").show(10);
            }
            SparkRuntime.initSparkResultTempView("spark" + table_name, DB, "info_" + table_name);
//            执行的hivesql为
            String hiveSql = getOverwriteSql(job, fileds, "spark" + table_name, 0).toString();
            System.out.println("执行的hivesql为：\n" + hiveSql);
//            SparkRuntime.overwriteHiveOnePartitionBySql("saprkAnalysisResult", "adp_dm", "ids_t_stat_tzfx", tjrq);
            SparkConfig.initStatic();
            SparkConfig.getSparkSession().sql(hiveSql);
//            SparkRuntime.exec(hiveSql);
            System.out.println(job.toString());
        }
    }

    /**
     * 获取写hive表sql
     *
     * @param job
     * @param fields
     * @param sparkView
     * @param rq
     * @return
     *
     *              table_name	sync_type	sync_col	jyr_dif
     *                txggl			1                       1
     */
    private static StringBuffer getOverwriteSql(Map<String, Object> job, List<String> fields, String sparkView, int rq) {
        StringBuffer sql = new StringBuffer();
        sql.append("insert overwrite table " + DB + "." + "info_" + job.get("table_name").toString());
        if (Integer.parseInt(job.get("sync_type").toString()) == 2) {
            sql.append(" partition("
                    + job.get("sync_col")
                    + "="
                    + rq
                    + ") select "
                    + joinFields(job, fields, false, false)
                    + " from " + sparkView + " where " + job.get("sync_col") + "=" + rq
            );

        } else if (Integer.parseInt(job.get("sync_type").toString()) == 1) {
            sql.append(" select "
                    + joinFields(job, fields, true, false)
                    + " from " + sparkView
            );
        }
        return sql;
    }

    /**
     * 转换spark临时表的编码
     *
     * @param job
     * @param fields
     * @param view_name table_name	sync_type	sync_col	jyr_dif
     *                  txggl	    	1                       1
     */
    private static void tranSparkTempViewCode(Map<String, Object> job, List<String> fields, String view_name) {
        StringBuffer sql = new StringBuffer();
        sql.append("select ");
        String str = joinFields(job, fields, true, true);
        sql.append(str);
        sql.append(" from " + view_name);
        SparkRuntime.registerTempView(sql.toString(), view_name);
    }

    /**
     * 获取select段sql
     *
     * @param job
     * @return table_name    sync_type	sync_col	jyr_dif
     * txggl			1                       1
     */
    private static StringBuffer getSelectSql(Map<String, Object> job, List<String> fields) {
        StringBuffer sql = new StringBuffer();
        String table_name = job.get("table_name").toString();
        sql.append("select ");
        sql.append(joinFields(job, fields, true, false));
        sql.append(" from " + MysqlDB + "." + table_name);
        if (Integer.parseInt(job.get("sync_type").toString()) == 2) { //增量数据
            int ksrq = tradingDays.getTradingDay(RQ, 0 - Integer.parseInt(job.get("jyr_dif").toString()));
            sql.append(" where " + job.get("sync_col").toString() + " between "
                    + ksrq
                    + " and " + RQ);
        }
        return sql;
    }

    /**
     * 拼接字段
     *
     * @param fields
     * @param withSyncCol
     * @return
     */
    private static String joinFields(Map<String, Object> job, List<String> fields, Boolean withSyncCol, Boolean tranCode) {
        int num = fields.size();
        String sync_col = "";
        try {
            sync_col = job.get("sync_col").toString();
        } catch (NullPointerException e) {
            sync_col = "";
        }
        //String sync_col = Optional.ofNullable(job.get("sync_col").toString()).orElse("");
        StringBuffer fieldStr = new StringBuffer();
        for (int i = 0; i < num; i++) {
            String field = fields.get(i);
            if (tranCode) {
                field = tranCodeField(field);
            }
            if (i != (num - 1)) {
                if (withSyncCol) {
                    fieldStr.append(field + ",");
                } else {
                    if (!sync_col.toUpperCase().equals(fields.get(i).toUpperCase())) {
                        fieldStr.append(field + ",");
                    }
                }
            } else {
                if (withSyncCol) {
                    fieldStr.append(field);
                } else {
                    if (!sync_col.toUpperCase().equals(fields.get(i).toUpperCase())) {
                        fieldStr.append(field);
                    }
                }
            }
        }
        return fieldStr.toString();
    }

    private static String tranCodeField(String field) {
        if (marketField.contains(field.toLowerCase())) {
//            使用udf函数转换交易所jysq
            System.out.println("tranMarketCode(" + field + ") as " + field);
            return "tranMarketCode(" + field + ") as " + field;
        } else if (currencyField.contains(field.toLowerCase())) {
            System.out.println("tranMarketCode(" + field + ") as " + field);
            return "tranCurrencyCode(" + field + ") as " + field;
        } else {
            return field;
        }
    }


    /**
     * 获取字段名
     *
     * @param table_name
     * @return
     */
    private static List<String> getFileds(String table_name) {
        ResultSet resultSet = MysqlUtil.executeSql("desc " + MysqlDB + "." + table_name);
        List<String> columns = new ArrayList<>();
        try {
            while (resultSet.next()) {
                columns.add(resultSet.getString("Field"));
            }
        } catch (Exception e) {
            logger.error("table doesn't exists!!!");
        }
        return columns;
    }

    /**
     * 初始化编码转换的Spark-sql-UDF
     */
    private static void initDicUDF() {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkConfig.spark.sparkContext());
        initMarketUDF(jsc);
        initCurrencyUDF(jsc);
    }

    /**
     * 币种编码转换
     *
     * @param jsc
     */
    private static void initCurrencyUDF(JavaSparkContext jsc) {
        Map<String, String> currency_code = new HashMap<String, String>() {
        };
        currency_code.put("RMB", "1");
        currency_code.put("USD", "3");
        currency_code.put("HKD", "2");
        final Broadcast<Map<String, String>> mapBroadcast = jsc.broadcast(currency_code);
        SparkConfig.spark.udf().register("tranCurrencyCode", new UDF1<String, String>() {
            @Override
            public String call(String src_code) throws Exception {
                Map<String, String> map = mapBroadcast.value();
                if (map.containsKey(src_code)) {
                    return map.get(src_code);
                } else {
                    return src_code;
                }
            }
        }, DataTypes.StringType);
    }

    /**
     * 交易所编码转换
     *
     * @param jsc
     */
    private static void initMarketUDF(JavaSparkContext jsc) {
        Map<String, String> market_code = new HashMap<String, String>() {
        };
        market_code.put("SH", "2");
        market_code.put("SZ", "1");
        market_code.put("HB", "4");
        market_code.put("SB", "3");
        market_code.put("TA", "5");
        market_code.put("TU", "6");
        market_code.put("HK", "8");
        market_code.put("7", "7");
        market_code.put("SK", "9");
        final Broadcast<Map<String, String>> mapBroadcast = jsc.broadcast(market_code);
        SparkConfig.spark.udf().register("tranMarketCode", new UDF1<String, String>() {
            @Override
            public String call(String src_code) throws Exception {
                Map<String, String> map = mapBroadcast.value();
                if (map.containsKey(src_code)) {
                    return map.get(src_code);
                } else {
                    return src_code;
                }
            }
        }, DataTypes.StringType);
    }
}
