package com.mobei.kafka.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//        // 是否⾃动提交offset，默认就是true
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        // ⾃动提交offset的间隔时间
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //1.创建⼀个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 指定分区消费
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));

        // 从头消费
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
        consumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));

        // 指定offset消费
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
        consumer.seek(new TopicPartition(TOPIC_NAME, 0), 10);

        // 指定时间消费
        List<PartitionInfo> topicPartitions = consumer.partitionsFor(TOPIC_NAME);
        //从1⼩时前开始消费
        long fetchDataTime = System.currentTimeMillis() - 1000 * 60 * 60;
        Map<TopicPartition, Long> map = new HashMap<>();
        for (PartitionInfo par : topicPartitions) {
            map.put(new TopicPartition(TOPIC_NAME, par.partition()),
                    fetchDataTime);
        }
        Map<TopicPartition, OffsetAndTimestamp> parMap =
                consumer.offsetsForTimes(map);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry :
                parMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            if (key == null || value == null) continue;
            Long offset = value.offset();
            System.out.println("partition-" + key.partition() +
                    "|offset-" + offset);
            System.out.println();
            //根据消费⾥的timestamp确定offset
            if (value != null) {
                consumer.assign(Arrays.asList(key));
                consumer.seek(key, offset);
            }
        }

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

//            //所有的消息已消费完
//            if (records.count() > 0) {
//                //有消息
//                // ⼿动同步提交offset，当前线程会阻塞直到offset提交成功
//                // ⼀般使⽤同步提交，因为提交之后⼀般也没有什么逻辑代码了
//                consumer.commitSync();//=======阻塞=== 提交成功
//            }

            //所有的消息已消费完
            if (records.count() > 0) {
                // ⼿动异步提交offset，当前线程提交offset不会阻塞，可以继续处理后⾯的程序逻辑
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            System.err.println("Commit failed for " + offsets);
                            System.err.println("Commit failed exception: " + exception.getStackTrace());
                        }
                    }
                });
            }
        }
    }

}
