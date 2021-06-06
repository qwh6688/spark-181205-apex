package com.apex.bigdata.java_01;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/2
 */
public class Array_AsList {
    public static void main(String[] args) {
//        List<String> asList = (ArrayList<String>)(Arrays.asList("aa", "bb", "cc"));
        List<String> asList = Arrays.asList("aa", "bb", "cc");

        System.out.println(asList.toString());
    }
}
