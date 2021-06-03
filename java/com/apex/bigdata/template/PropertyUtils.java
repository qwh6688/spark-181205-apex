package com.apex.bigdata.template;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.util.MutableURLClassLoader;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by Admin on 2018/3/28.
 * 读取配置文件工具类
 */
public class PropertyUtils {
    public static MyProPerties arguments = new MyProPerties();
    ;
    public static MyProPerties confs = new MyProPerties();

    public static String classParh = "spark.properties";

    public static String envParh = "public.properties";

    //加载顺序  程序入参FILE >  envParth > classParth
    private static void loadProperties() throws Exception {
        System.out.println("开始输出配置文件信息拉！！！！！");
        ClassLoader classLoader =  Thread.currentThread().getContextClassLoader();
        System.out.println("classLoader:"+classLoader);
        if (classLoader != null) {
            InputStream inputStream = classLoader.getResourceAsStream(classParh);
            System.out.println("inputStream:"+inputStream);
            if (inputStream != null) {
                System.out.println("加载类配置文件信息:" + classParh);
                confs.load(inputStream);
                System.out.println("CONFS内容是！"+confs);
                inputStream.close();
            }
            if(classLoader instanceof MutableURLClassLoader){
                for (URL url : ((MutableURLClassLoader)classLoader).getURLs()) {
                    if(url.getPath().endsWith(envParh)){
                        System.out.println("加载环境默认文件信息:" + envParh);
                        confs.load(url.openStream());
                    }
                }
                System.out.println("第二个if的confs："+confs);
            }

        }
        //FILE配置信息 过程此处要把file放在--jar 参数后面
        getProperty(arguments.getProperty("FILE"));
    }

    //获取配置文件
    private static Properties getProperty(String path) throws Exception {
        if (path != null && !"".equals(path)) {
            ClassLoader classLoader =  Thread.currentThread().getContextClassLoader();
           // MutableURLClassLoader ret = (MutableURLClassLoader) Thread.currentThread().getContextClassLoader();
            if (classLoader != null) {
                String[] paths = path.split(File.separator);
                String fileName = paths[paths.length - 1];
                System.out.println("filename是："+fileName);
                if(classLoader instanceof MutableURLClassLoader){
                    for (URL url : ((MutableURLClassLoader)classLoader).getURLs()) {
                        System.out.println("加载配置入参文件信息:" + fileName);
                        if(url.getPath().endsWith(fileName)){
                            confs.load(url.openStream());
                        }
                    }
                    System.out.println("第三次conf是："+confs);
                }else{
                    File confFile = new File(path);
                    System.out.println("conffile是！："+confFile);
                    if(confFile.exists()){
                        confs.load(new FileInputStream(confFile));
                    }
                    System.out.println("第四次confs"+confs);
                }
            }
        }
        return confs;
    }

    public static void parseArgs(String[] args) throws Exception{
        if (args.length > 0) {
            String arg = args[0];
            String[] argList = arg.split("&");
            for (int i = 0; i < argList.length; i++) {
                String[] ss = argList[i].split("=");
                if(ss.length>1){
                    arguments.put(argList[i].split("=")[0].toUpperCase(), argList[i].split("=")[1]);
                }
            }
            System.out.println("------arguments，字典参数是！！！");
            System.out.println(arguments);
        }
        try {
            loadProperties();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static String parseCustNo(String string){
        String[] strings = string.split(",");
        String tmp = "";
        for (int i=0; i<strings.length; i++){
            if (i < strings.length-1){
                tmp = tmp + "'" + strings[i] + "',";
            }else {
                tmp = tmp + "'" + strings[i] + "'";
            }
        }
        return tmp;
    }
    public static int getCusts(){
        if (StringUtils.isBlank(arguments.get("KHH"))){
            return 0;
        }else {
            String[] strings = arguments.get("KHH").split(",");
            return strings.length;
        }
    }
}
