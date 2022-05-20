package com.xxx.producers;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 消息生产者代码示例
 */
public class ProducerDemo {

    public static void main(String[] args) {

        // 设置配置属性
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 300);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建producer
        KafkaProducer producer= new KafkaProducer(props);
        System.out.println("创建");
        // 产生并发送消息

        long start = System.currentTimeMillis();
        int events = 100;
//        for (long i = 0; i < events; i++) {

            while (true) {
                int i= (int) Math.round(Math.random()*20);
            long runtime = new Date().getTime();
            String ip = "10.68.78." + i;//rnd.nextInt(255);
            String msg = "www.xxx.com," + ip;
            //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0

            ProducerRecord<String, String> record = new ProducerRecord<>("kafka_test_topic", msg);
            long _s = System.currentTimeMillis();
            producer.send(record);
            System.out.println("发送 "+msg);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
//        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
//        producer.close();
    }

}

