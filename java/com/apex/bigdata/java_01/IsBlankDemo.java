package com.apex.bigdata.java_01;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.Properties;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/1
 */


public class IsBlankDemo {
    public static void main(String[] args) {
        String str = "";
        System.out.println(str.length());
        //isBlank 判断字符串是否为null 或者""
        if (StringUtils.isBlank(str)){
            System.out.println("为空！！！！");
        }else
        {
            System.out.println("不为空");
        }
    }

    @Test
    public void prop(){
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");
        System.out.println("user: " +properties.getProperty("user")+"\t"+"passeord: "+properties.getProperty("passeord"));
    }

}
