package com.mobei.kafka.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka同步发送消息
 *
 * @author liuyaowu
 * @date 2021/11/2113:43
 * @remark
 */
@Slf4j
public class MySimpleSyncProducer {

    public static final String TOPIC_NAME = "my-simple-topic";
    private final static String CONSUMER_GROUP_NAME = "testGroup";

    public static void main(String[] args) throws Exception {
        // 1.设置参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.229.20:9092");
        // 把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "1");
        /**
         * 发送失败会重试，默认重试间隔100ms，重试能保证消息发送的可靠性，但是也可能造
         * 成消息重复发送，⽐如⽹络抖动，所以需要在接收者那边做好消息接收的幂等性处理
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //重试间隔设置
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);

        // kafka默认会创建⼀个消息缓冲区，⽤来存放要发送的消息，缓冲区是32m
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // kafka本地线程会去缓冲区中⼀次拉16k的数据，发送到broker
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 如果线程拉不到16k的数据，间隔10ms也会将已拉到的数据发到broker
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        // 2.创建⽣产消息的客户端，传⼊参数
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 3.创建消息
        //key：作⽤是决定了往哪个分区上发，value：具体要发送的消息内容
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "mykeyvalue", "hellokafka1");

        //4.发送消息,得到消息发送的元数据并输出
        RecordMetadata metadata = producer.send(producerRecord).get();
        log.error("同步⽅式发送消息结果：topic-{}|partition-{}|offset-{}", metadata.topic(), metadata.partition(), metadata.offset());
    }

}
