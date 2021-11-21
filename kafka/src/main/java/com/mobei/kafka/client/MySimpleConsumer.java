package com.mobei.kafka.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author liuyaowu
 * @date 2021/11/2116:27
 * @remark
 */
@Slf4j
public class MySimpleConsumer {
    private final static String TOPIC_NAME = "my-simple-topic";
    private final static String CONSUMER_GROUP_NAME = "testGroup";
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.229.20:9092");
        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());

        // 是否⾃动提交offset，默认就是true
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // ⾃动提交offset的间隔时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //1.创建⼀个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer< >(props);
        //2. 消费者订阅主题列表
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            /*
             * 3.poll() API 是拉取消息的⻓轮询
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                //4.打印消息
                log.error("收到消息：partition = {},offset = {}, key = {}, value = {}",
                        record.partition(), record.offset(), record.key(), record.value());
            }

        }
    }

}
