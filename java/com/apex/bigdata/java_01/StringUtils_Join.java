package com.apex.bigdata.java_01;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Array;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class StringUtils_Join {
    public static void main(String[] args) {
        String[] strArr ={"1","2","3","4"};
        String sql = "upsert into " + "APEX" + "." + "INFO_THIS_JJJZ_ETF"  + " values(%s)";// ON DUPLICATE KEY IGNORE
        String joinStr = StringUtils.join(strArr, ",");
        String combineStr = String.format(sql, joinStr);
        System.out.println("拼凑后的结果： "+combineStr);


    }
}
