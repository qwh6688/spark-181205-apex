package com.apex.bigdata.template.MyEnum;

public enum Conf {

    mysql_bd("mysql.bd"),

    kerberos("kerberos"),

    spark("spark"),

    redis("redis"),

    kafka("kafka"),

    phoenix("phoenix"),

    hbase("hbase"),

    hdfs("hdfs"),

    elasticsearch("es"),

    hq("hq"),

    ls_jzjy("ls.jzjy.oracle.mnch"),

    ls_xy("ls.xy.oracle.mnch"),
    ;

    private Conf(String prefix){
        this.prefix = prefix;
    }
    public String prefix;
}
