package com.apex.bigdata.util;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class LoadPhoenix {

    public static Connection getConnection() throws IOException {
        Properties props = new Properties();
        props.load(LoadPhoenix.class.getResourceAsStream("/properties.properties"));
        try {
            // load driver
            Class.forName(props.getProperty("phoenix.driver"));
            // jdbc 的 url 类似为 jdbc:phoenix [ :<zookeeper quorum> [ :<port number> ] [ :<root node> ] ]，
            return DriverManager.getConnection(props.getProperty("phoenix.jdbc.url"));
        } catch (Exception e) {
            e.printStackTrace();
//            e.fillInStackTrace();
            return null;
        }


    }
}
