package com.apex.bigdata.java_01;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/3
 */
public class StringFormat {
    public static void main(String[] args) {

//        %s  字符串类型
//        %d  整数类型（十进制）
         String formatStr = String.format("insert overwrite table %s.%s partition(%s=%d) select ",
                "adp_cfg",
                "info_this_jjjz_etf",
                "jzrq",
                20210602);
        System.out.println(formatStr);

    }
}
