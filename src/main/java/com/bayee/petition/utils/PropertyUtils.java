package com.bayee.petition.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * @Description: TODO
 * @Author YueCang
 * @Date 2021/1/2614:55
 */
public class PropertyUtils {
    private static final Logger log = Logger.getLogger(PropertyUtils.class);

    private static Properties prop = new Properties();

    //加载驱动,使用静态块，只需要加载一次
    static {
        try {
            /**
             * 加载配置文件的两种方法，任选一种都可以
             */
            //pro.load(DBUtil.class.getResourceAsStream("/DBUtil.properties"));
            prop.load(PropertyUtils.class.getClassLoader().getResourceAsStream("config.properties"));
            log.info("Load config.properties successfully");
        } catch (IOException e) {
            log.info("Load config.properties failed");
            e.printStackTrace();
        } catch (Exception e) {
            log.info("Load config.properties failed");
            e.printStackTrace();
        }

    }

    public static String getValue(String key){
        return prop.getProperty(key);
    }

    public static void main(String[] args) {
        String hbaseManagerUrl = PropertyUtils.getValue("HbaseManagerUrl");
        System.out.println(hbaseManagerUrl);
    }
}
