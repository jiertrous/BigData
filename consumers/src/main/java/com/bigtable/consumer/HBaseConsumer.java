package com.bigtable.consumer;

import com.bigtable.utils.PropertityUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class HBaseConsumer {
    public static void main(String[] args) throws Exception{
        // 获取kafka配置信息
        Properties properties = PropertityUtil.getPropertity();
        // 创建kafka消费者订阅数据
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //KafkaConsumer<Object, Object>kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(properties.getProperty(
                "kafka.topics")));
        // 不需要trycatch ，这一步错了，就挑出，无法下一步进去
        HBaseDAO hBaseDAO = new HBaseDAO();
        // 循坏拉去数据并打印数据
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(
                        100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    // put
                    hBaseDAO.put(consumerRecord.value());
                }
            }
        } finally{ // 防止上面的whil循环挂掉，可以让缓存的数据放入hbase，
            hBaseDAO.close();
        }
    }
}
