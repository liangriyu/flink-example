package com.landy.flink.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author liangriyu
 * @description: 配置文件类
 * @date 2019/9/3
 */
public class PropUtil {

    public  Properties getProperties(String fileName) throws IOException {
        Properties properties = new Properties();
//        properties.load(ClassLoader.getSystemResourceAsStream(fileName));
        properties.load(this.getClass().getClassLoader().getResourceAsStream(fileName));
        return properties;

    }

    public  Properties getProperties() throws IOException {
        return getProperties("application.properties");
    }

}