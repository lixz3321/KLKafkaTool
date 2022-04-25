package kl.utils;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/24/9:31
 * @Description: 用于加载不同的系统配置，从不同的配置中获取参数
 */
public class KafkaConf {
    /*
    系统配置参数集
     */
    private HashMap<String,String> params = null;

    /**
     * 构造
     * @param conf
     */
    public KafkaConf(String conf){
        params = new HashMap<>();
        ResourceBundle bundle = ResourceBundle.getBundle(conf);
        Set<String> setKey = bundle.keySet();
        Iterator<String> iter = setKey.iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String value = bundle.getString(key);
            params.put(key,value);
        }
    }

    /**
     * 获取参数
     * @param key
     * @return
     */
    public String get(String key){
        return params.get(key);
    }

    /**
     * 获取参数
     * @param key
     * @return
     */
    public String getOrDefault(String key,String value){
        return params.getOrDefault(key,value);
    }

    /**
     * 返回以Properties对象封装的参数集
     * @return
     */
    public Properties getProperties(){
        Properties props = new Properties();
        for (String key:params.keySet()){
            props.put(key,params.get(key));
        }
        return props;
    }

    /**
     * 测试
     * @param args
     */
    public static void main(String[] args){
        KafkaConf conf = new KafkaConf("cdh-prod-consumer");
        System.out.println(conf.get("group.id"));
    }
}
