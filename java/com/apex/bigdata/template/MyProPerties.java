package com.apex.bigdata.template;

import com.apex.bigdata.template.MyEnum.Conf;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class MyProPerties extends Properties {

    public final static String PASSWORD_KEY  ="password";

    public String get(String key){
        Object oval= super.getOrDefault(key,null);
        String sval = (oval instanceof String) ? (String)oval : null;
        return ((sval == null) && (defaults != null)) ? defaults.getProperty(key) : sval;
    }


    /**
     * 获取去除前缀的配置信息
     * 使用此方法需要注意 key=prefix 项不会加载获取到
     * @param conf
     * @return
     */
    public MyProPerties getConfWithNoPrefix(Conf conf){
        return getConf(conf.prefix,true);
    }

    /**
     * 获取包含前缀的配置信息
     * @param conf
     * @return
     */
    public MyProPerties getConf(Conf conf){
        return getConf(conf.prefix,false);
    }

    /**
     * 获取子配置信息
     * 遇到密码加密情况时自动解密
     * @param prefix 配置前缀
     * @param noPrefix 是否包含配置前缀 false 好含 true不包含
     * @return
     */
    public MyProPerties getConf(String prefix,boolean noPrefix){
        MyProPerties properties = new MyProPerties();
        Iterator<Map.Entry<Object, Object>> it = this.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<Object, Object> obj = it.next();
            String key = obj.getKey().toString();
            if(key.startsWith(prefix)){
                //遇到密码自动解密操作
                Object value = obj.getValue();
                if(key.endsWith(MyProPerties.PASSWORD_KEY)){
                    value = DefaultEncryptor.parsePassword(value+"");
                }
                //不包含前缀情况
                if(noPrefix && !key.equals(prefix)) {
                    properties.put(key.substring(prefix.length()+1),value);
                }else if(!noPrefix){
                    properties.put(key,value);
                }
            }
        }
        return properties;
    }
    

}
