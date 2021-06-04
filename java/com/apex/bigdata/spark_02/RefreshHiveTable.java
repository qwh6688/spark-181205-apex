package com.apex.bigdata.spark_02;

import com.apex.bigdata.template.SparkConfig;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class RefreshHiveTable {
    private static String dbName;
    private static String targetTableName;
    public static void main(String[] args) {
        SparkConfig.getSparkSession().catalog().refreshTable(dbName + "." + targetTableName);

    }
}
