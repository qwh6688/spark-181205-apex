package com.apex.bigdata.util.test;

import com.apex.bigdata.template.SparkRuntime;
import com.apex.bigdata.util.DateUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/2
 */
public class Demo01 {
    private static int j1z_qc;
    private static int j1y_qc;
    private static int j3y_qc;
    private static int jbn_qc;
    private static int j1n_qc;
    private static int tjrq;
    private static String khh;
    private static int syrq;
    private static String sqlBuf = "";
    private static int debug = 0;
    private static Logger logger = LoggerFactory.getLogger(Demo01.class);

    public static void main(String[] args) throws ParseException {
        demo01();
    }



    public static void demo01() throws ParseException {
        tjrq = 20210531;
        syrq = DateUtil.getDate(tjrq, -1);
        logger.info("syrq：" + syrq);
        //获取近X日前一日的日期
        j1z_qc = DateUtil.getDate(tjrq, -7);
        logger.info("j1z_qc：" + j1z_qc);

        j1y_qc = DateUtil.getDate(tjrq, -30);
        j3y_qc = DateUtil.getDate(tjrq, -90);
        jbn_qc = DateUtil.getDate(tjrq, -182);
        j1n_qc = DateUtil.getDate(tjrq, -365);

    }
}
