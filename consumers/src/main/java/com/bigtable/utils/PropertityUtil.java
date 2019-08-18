package com.bigtable.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertityUtil {
    static Properties properties = null;
    public static Properties getPropertity(){
        InputStream is = ClassLoader.getSystemResourceAsStream("kafka.properties");
        try {
            properties = new Properties();
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return properties;
    }

}
