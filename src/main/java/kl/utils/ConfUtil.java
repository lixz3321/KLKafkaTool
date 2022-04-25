package kl.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/24/9:10
 * @Description:
 */
public class ConfUtil {
    /*
    系统配置文件
     */
    public static final String envConf = "conf";
    /*
    系统配置参数集
     */
    public static HashMap<String,String> sysParams = null;

    /**
     * 从properties中加载配置参数
     */
    public static void loadConf(){
        sysParams = new HashMap<>();
        ResourceBundle bundle = ResourceBundle.getBundle(envConf);
        Set<String> setKey = bundle.keySet();
        Iterator<String> iter = setKey.iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String value = bundle.getString(key);
            sysParams.put(key,value);
        }
    }

    /**
     * 获取系统参数
     * @param key 键
     * @return
     */
    public static String getString(String key){
        if(sysParams == null){
            loadConf();
        }
        String param = null;
        try {
            param = sysParams.get(key);
            return param;
        }catch (Exception NoSuchElementException){
            return param;
        }
    }

    /**
     * 获取系统参数
     * @param key 键
     * @param dvalue 默认值
     */
    public static String getString(String key,String dvalue){
        String param = getString(key);
        if(dvalue!=null && param==null){
            param = dvalue;
        }
        return param;
    }

    /**
     * 获取系统参数
     * @param key 键
     * @return
     */
    public static Integer getInt(String key){
        String param = getString(key);
        if(param!=null && !param.equals("")){
            return Integer.valueOf(param);
        }else{
            return null;
        }
    }

    /*
    测试
     */
    public static void main(String[] args){
        System.out.println(getString("p2"));
    }
}
