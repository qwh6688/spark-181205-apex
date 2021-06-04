package com.apex.bigdata.spark_02;

import com.apex.bigdata.template.PhoenixUtils;
import com.apex.bigdata.util.LoadPhoenix;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Description:
 *
 * @author quwh
 * @date Created on 2021/6/4
 */
public class PhoenixTest {
    public static void main(String[] args) {
        //查询
//        query();
        //插入
        upsert();
    }


    private static void upsert() {

        Connection conn = null;
        try {
            // get connection
            conn = LoadPhoenix.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // create sql
            String sql = "upsert into TEST.\"student\" (id,name,age,date) values('1006','xupanli','6688','2021-06-04')";

            PreparedStatement ps = conn.prepareStatement(sql);
            // execute upsert
            System.out.println("更改的记录数： " + ps.executeUpdate());
            String msg = ps.executeUpdate() > 0 ? "insert success..."
                    : "insert fail...";
            // you must commit
            conn.commit();
            System.out.println(msg);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static void query() {

        Connection conn = null;
        try {
            // get connection
            conn = LoadPhoenix.getConnection();

            // check connection
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }

            // create sql
            String sql = "select * from TEST.\"student\"";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                while (rs.next()) {
                    System.out.print(rs.getString("ID") + "\t");
                    System.out.print(rs.getString("NAME") + "\t");
                    System.out.println(rs.getString("AGE"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}
