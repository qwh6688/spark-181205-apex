package com.apex.bigdata.template;

 import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class PhoenixUtils {
    private static String user;
    private static String password;
    private static String url;
    private static String driver;
    private static Connection connection;
    private static Properties props ;

    public PhoenixUtils() {
    }

    public static void init() throws IOException {
        props = new Properties();
        props.load(PhoenixUtils.class.getResourceAsStream("/properties.properties"));

        try {
            driver = props.getProperty("phoenix.driver");
            Class.forName(driver);
            user = "";
            password = "";
            url = props.getProperty("phoenix.jdbc.url");
            if (props.getProperty("phoenix.auth").equals("true")) {
                user = props.getProperty("phoenix.user");
                password = props.getProperty("phoenix.password");
            }
            connection = DriverManager.getConnection(url, user, password);
            System.out.println(driver + "\t" + url);
            System.out.println(connection);
        } catch (Exception var1) {
            var1.printStackTrace();
        }

    }

    public static Connection getNewConnection() throws Exception {
        Properties props = new Properties();
        props.load(PhoenixUtils.class.getResourceAsStream("/properties.properties"));
        driver = props.getProperty("phoenix.driver");
        String user = "";
        String password = "";
        try {
            Class.forName(driver);
            if (props.getProperty("phoenix.auth").equals("true")) {
                user = props.getProperty("phoenix.user");
                password = props.getProperty("phoenix.password");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Connection connection = DriverManager.getConnection(url);
        return connection;
    }

    public static void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static List<String> getTableField(String namespace, String table) {
        List<String> fields = new ArrayList<>();
        try {
            String sql = "select column_name,column_def from system.catalog where table_schem='%s' and table_name='%s' and column_name is not null";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(sql, namespace.toUpperCase(), table.toUpperCase()));
            while (resultSet.next()) {
                fields.add(resultSet.getString("column_name").toLowerCase());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("------------------");
        System.out.println(fields);
        return fields;
    }

    private static String getUpsertSql(String namaspace, String table, List<String> fields) {
        String sql = "upsert into " + namaspace + "." + table + "(%s)" + " values(%s)";// ON DUPLICATE KEY IGNORE
        String[] quote = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            quote[i] = "?";
        }
        return String.format(sql,
                StringUtils.join(fields, ","),
                StringUtils.join(quote, ","));

    }


    public static void saveToPhoenix(Dataset<Row> ds, final String namaspace, final String table) {
        LongAccumulator executeNum = SparkConfig.javaSparkContext.sc().longAccumulator(table + "_num");
        final Broadcast<List<String>> broadcastPhoenixFields = SparkConfig.javaSparkContext.broadcast(getTableField(namaspace, table));
//        final Broadcast<MyProPerties> broadcastConf = SparkConfig.javaSparkContext.broadcast(PropertyUtils.confs.getConfWithNoPrefix(Conf.phoenix));
        final Broadcast<String> broadcastSql = SparkConfig.javaSparkContext.broadcast(getUpsertSql(namaspace, table, broadcastPhoenixFields.getValue()));
        ds.repartition(Integer.valueOf(props.getProperty("spark.default.parallelism")))
                .foreachPartition(new ForeachPartitionFunction<Row>() {
                    @Override
                    public void call(Iterator<Row> rows) throws Exception {
                        List<String> phoenixFields = broadcastPhoenixFields.getValue();
//                MyProPerties confs = broadcastConf.getValue();
                        String sql = broadcastSql.getValue();
                        //Class.forName(confs.get("phoenixDriver"));
                        Connection connection = getNewConnection();
                        PreparedStatement preparedStatement = connection.prepareStatement(sql);
                        while (rows.hasNext()) {
                            Row row = rows.next();
                            for (int i = 1; i <= phoenixFields.size(); i++) {
                                try {
                                    preparedStatement.setString(i, row.getAs(phoenixFields.get(i - 1)) == null ? "" : row.getAs(phoenixFields.get(i - 1)).toString());
                                } catch (IllegalArgumentException e) {
                                    preparedStatement.setString(i, "");
                                }
                            }
                            try {
                                preparedStatement.executeUpdate();
                            } catch (Exception e) {

                            } finally {
                                // must commit
                                connection.commit();
                                executeNum.add(1);
                            }
                        }
                        connection.close();
                    }
                });
        System.out.println("执行的记录条数: " + executeNum );
    }


}
