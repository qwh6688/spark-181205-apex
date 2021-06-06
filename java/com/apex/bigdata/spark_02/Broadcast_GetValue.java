package com.apex.bigdata.spark_02;

import com.apex.bigdata.template.SparkConfig;
import com.apex.bigdata.template.SparkRuntime;
import org.apache.spark.broadcast.Broadcast;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class Broadcast_GetValue {
    public static void main(String[] args) {
        //广播变量使用。。。
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        SparkConfig.initStatic();;
        final Broadcast<List<String>> broadcastPhoenixFields = SparkConfig.javaSparkContext.broadcast(list);
        List<String> broFields = broadcastPhoenixFields.getValue();
        System.out.println(broFields.toString());


    }
}
