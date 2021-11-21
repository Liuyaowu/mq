package com.mobei.kafka.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka异步发送消息
 *
 * @author liuyaowu
 * @date 2021/11/2113:43
 * @remark
 */
@Slf4j
public class MySimpleAsyncProducer {

    public static final String TOPIC_NAME = "my-simple-topic";
    private final static String CONSUMER_GROUP_NAME = "testGroup";

    public static void main(String[] args) throws Exception {
        // 1.设置参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.229.20:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        // 把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2.创建⽣产消息的客户端，传⼊参数
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 3.创建消息
        //key：作⽤是决定了往哪个分区上发，value：具体要发送的消息内容
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "mykeyvalue1", "异步发送的消息111");

        // 4.异步发送消息,得到消息发送的元数据并输出
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("发送消息失败：" + exception.getStackTrace());
                }
                if (metadata != null) {
                    log.error("异步⽅式发送消息结果：topic-{}|partition-{}|offset-{}", metadata.topic(),
                            metadata.partition(), metadata.offset());
                }
            }
        });

        Thread.sleep(10 * 1000);
    }

}
