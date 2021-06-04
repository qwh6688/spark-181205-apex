package com.apex.bigdata.java_01;

import java.io.IOException;
import java.util.Properties;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class PropertiesDemo {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(PropertiesDemo.class.getResourceAsStream("/spark.properties"));
        //文件内容
        System.out.println(props.toString());
        String phoenixAuth = props.getProperty("phoenix.driver");
        System.out.println(phoenixAuth);


    }
}
