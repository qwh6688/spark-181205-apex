package com.apex.bigdata.template;

import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class MysqlUtil {

    private Connection conn;

    private String logId;

    private long ID;
    private Date startTime;
    public void setLogId(String logId) {
        this.logId = logId;
    }

    private SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    public MysqlUtil(Properties properties) {
        try{
            System.out.println("properties是这个！"+properties);
            System.out.println(properties.getProperty("url"));
            Class.forName(properties.getProperty("driver"));
            conn = DriverManager.getConnection(properties.getProperty("url"),
                    properties.getProperty("user"),
                    properties.getProperty("password"));

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void logBegin(String desc, String name, int rq, Date before, int status, String note, int runType, String channel) throws SQLException {
        try{
            startTime = new Date();
            String sql = "INSERT INTO pub_sys.t_etl_proc_log ( " +
                    " DETAIL_LOG_ID, " +
                    " PROC_DESC, " +
                    " PROC_NAME, " +
                    " STAT_DATE, " +
                    " COST_TIME, " +
                    " START_TIME, " +
                    " END_TIME, " +
                    " STATUS, " +
                    " NOTE, " +
                    " RUN_TYPE, " +
                    " CHANNEL " +
                    ") " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1,logId);
            preparedStatement.setString(2,desc);
            preparedStatement.setString(3,name);
            preparedStatement.setInt(4,rq);
            preparedStatement.setDouble(5, 0);
            preparedStatement.setString(6,format.format(startTime));
            preparedStatement.setString(7,null);
            preparedStatement.setInt(8,1);
            preparedStatement.setString(9, StringUtils.isBlank(note) ? "spark程序明细": note);
            preparedStatement.setInt(10,runType);
            preparedStatement.setString(11,channel);
            preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next()){
                ID = resultSet.getLong(1);
            }
        }catch (Exception e){
            System.out.println("过程-------" + name + "-------记录日志失败");
            System.out.println("desc = [" + desc + "], Name = [" + name + "], rq = [" + rq + "], before = [" + before + "], status = [" + status + "], note = [" + note + "], runType = [" + runType + "], channel = [" + channel + "]");
            e.printStackTrace();
//            System.exit(1);
        }
    }
    public void logEnd(String desc, String name, int rq, Date before, int status, String note, int runType, String channel) throws Exception {
        try{
            Date now = new Date();
            double cost = Math.ceil((now.getTime() - startTime.getTime()) / 1000);
            PreparedStatement preparedStatement = conn.prepareStatement("update pub_sys.t_etl_proc_log set" +
                    " COST_TIME=?," +
                    " END_TIME=?," +
                    " STATUS=?," +
                    " NOTE=?" +
                    " where id=?");
            preparedStatement.setDouble(1, cost);
            preparedStatement.setString(2, format.format(now));
            preparedStatement.setInt(3, status);
            preparedStatement.setString(4, StringUtils.isBlank(note) ? "spark程序明细": note);
            preparedStatement.setLong(5, ID);
            preparedStatement.execute();
        }catch (Exception e){
            System.out.println("过程-------" + name + "-------记录日志失败");
            System.out.println("desc = [" + desc + "], Name = [" + name + "], rq = [" + rq + "], before = [" + before + "], status = [" + status + "], note = [" + note + "], runType = [" + runType + "], channel = [" + channel + "]");
            e.printStackTrace();
//            System.exit(1);
        }
    }
    public void log2Sql(String desc, String name, int rq, Date before, int status, String note, int runType, String channel) throws SQLException {
        try {
            String begin = format.format(before);
            Date now = new Date();
            double cost = Math.ceil((now.getTime() - before.getTime()) / 1000);
            String end = format.format(now);

            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO pub_sys.t_etl_proc_log ( " +
                    " DETAIL_LOG_ID, " +
                    " proc_desc, " +
                    " PROC_DESC, " +
                    " STAT_DATE, " +
                    " COST_TIME, " +
                    " START_TIME, " +
                    " END_TIME, " +
                    " STATUS, " +
                    " NOTE, " +
                    " RUN_TYPE, " +
                    " CHANNEL " +
                    ") " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            preparedStatement.setString(1,logId);
            preparedStatement.setString(2,desc);
            preparedStatement.setString(3,name);
            preparedStatement.setInt(4,rq);
            preparedStatement.setDouble(5,cost);
            preparedStatement.setString(6,begin);
            preparedStatement.setString(7,end);
            preparedStatement.setInt(8,status);
            preparedStatement.setString(9,note);
            preparedStatement.setInt(10,runType);
            preparedStatement.setString(11,channel);
            preparedStatement.execute();

        } catch (Exception e) {
            System.out.println("过程-------" + name + "-------记录日志失败");
            System.out.println("desc = [" + desc + "], Name = [" + name + "], rq = [" + rq + "], before = [" + before + "], status = [" + status + "], note = [" + note + "], runType = [" + runType + "], channel = [" + channel + "]");
            e.printStackTrace();
        }
    }

    public ResultSet executeSql(String sql){
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;

    }
    public void close(){
        try{
            conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
