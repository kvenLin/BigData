package com.imooc.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * @author louye
 * @title: ParamUtils
 * @description: 读取属性配置文件
 * @date 2019-06-24,21:21
 */
public class ParamUtils {
    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamUtils.class.getClassLoader().getResourceAsStream("wordCount.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() throws Exception {
        return properties;
    }
}
