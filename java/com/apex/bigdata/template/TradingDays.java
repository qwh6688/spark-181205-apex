package com.apex.bigdata.template;

import com.google.common.primitives.Ints;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zhangwz on 2018/2/4.
 * 系统交易日
 */
public class TradingDays implements java.io.Serializable{
    private static final long serialVersionUID = -123L;
    private static final Logger log = LoggerFactory.getLogger(TradingDays.class);

    // --直接使用int数组，搜索更方便高效
    private static int[] arZrr;
    private static int[] arJyr;
    private Integer zrr;
    private Integer jyr;
    private Broadcast<int[]> broadcastZrr;
    private Broadcast<int[]> broadcastJyr;
    public TradingDays() {
    }

    public TradingDays(final SparkSession spark) {
        try {
            JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
            String adpVersion = PropertyUtils.confs.get("ids.version.adp");
            List<Row> rows;
            if (StringUtils.isBlank(adpVersion)){
                rows = spark.sql("select cast(zrr as int) zrr, cast(jyr as int) jyr from dsc_cfg.t_xtjyr order by zrr").collectAsList();
            }else {
                rows = spark.sql("select cast(zrr as int) zrr, cast(jyr as int) jyr from adp_cfg.t_xtjyr order by zrr").collectAsList();
            }


            List<Integer> lsZrr = new ArrayList<Integer>();
            List<Integer> lsJyr = new ArrayList<Integer>();
            for (Row row : rows) {
                //排除自然日与交易日不相等的数据，否则getTradingDay函数返回值有可能不是交易日
                zrr = row.getInt(0);
                jyr = row.getInt(1);
                lsZrr.add(zrr);
                lsJyr.add(jyr);
            }

            arZrr = Ints.toArray(lsZrr);
            arJyr = Ints.toArray(lsJyr);
            broadcastZrr = jsc.broadcast(arZrr);
            broadcastJyr = jsc.broadcast(arJyr);
            log.info("load xtjyr competed! size:" + arZrr.length);
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            arZrr = new int[0];
            arJyr = new int[0];
        }
    }


    public int getTradingDay(Integer ndate, Integer days) {
        int[] arZrr;
        int[] arJyr;
        try {
            arJyr = broadcastJyr.value();
            arZrr = broadcastZrr.value();
        }catch (NullPointerException e){
            arJyr = this.arJyr;
            arZrr = this.arZrr;
        }
        int idx = Arrays.binarySearch(arZrr, ndate);
        int len = arZrr.length;
        int n;
        if (idx >= 0 && idx < len) {
            int jyr = arJyr[idx];
            if (ndate != jyr && days < 0){
                n = (-days) - 1;
            }else{
                n = (days < 0) ? -days : days;
            }
            while (n != 0) {
                idx = (days > 0) ? idx + 1 : idx - 1;
                if (idx < 0 || idx >= len)
                    break;
                int jyr2 = arJyr[idx];
                if (jyr != jyr2) {
                    jyr = jyr2;
                    n--;
                }
            }
            return jyr;
        }
        return ndate;


    }
    public List<Integer> getTradingDays(Integer ksrq, Integer jsrq){
        int[] Zrr;
        int[] Jyr;
        try {
            Jyr = broadcastJyr.value();
            Zrr = broadcastZrr.value();
        }catch (NullPointerException e){
            Jyr = this.arJyr;
            Zrr = this.arZrr;
        }
        List days = new ArrayList();
        int indexStart = Arrays.binarySearch(Zrr, ksrq);
        int indexEnd = Arrays.binarySearch(Zrr, jsrq);

        if (Zrr[indexStart] != Jyr[indexStart]){
            indexStart = Arrays.binarySearch(Zrr, getTradingDay(ksrq, 1));
        }
        for (int idx=indexStart; idx<=indexEnd; idx++){
            if (Zrr[idx] == Jyr[idx]){
                days.add(Jyr[idx]);
            }
        }
        return days;
    }
}
