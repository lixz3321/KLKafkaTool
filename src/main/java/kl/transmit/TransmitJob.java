package kl.transmit;

import kl.utils.ConfUtil;
import kl.utils.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lxiz
 * @Date: 2022/04/22/16:41
 * @Description: kafka消息跨集群转发
 */
public class TransmitJob {

    public static Logger LOG = Logger.getLogger(TransmitJob.class);
    /**
     * 执行入口
     * @param args
     */
    public static void main(String[] args){
        String consumerConfName = ConfUtil.getString("kafka.consumer.conf");
        String producerConfName = ConfUtil.getString("kafka.producer.conf");
        LOG.info("当前使用的kafka消费者配置："+consumerConfName+",生产者配置："+producerConfName);
        TransmitJob.run(consumerConfName,producerConfName);
    }

    /**
     * 执行作业
     * @param kafkaConf1 kafka消费者配置
     * @param kafkaConf2 kafka生产者配置
     */
    public static void run(String kafkaConf1,String kafkaConf2){
        //消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(new KafkaConf(kafkaConf1).getProperties());
        //生产者
        Producer<String, String> producer = new KafkaProducer<String,String>(new KafkaConf(kafkaConf2).getProperties());
        //读/写 topic
        String source_topic = ConfUtil.getString("kafka.source.topic");
        String target_topic = ConfUtil.getString("kafka.target.topic");
        LOG.info("输入主题："+source_topic+",输出主题："+target_topic);
        //订阅topic
        consumer.subscribe(Arrays.asList(source_topic));
        LOG.info("输入主题订阅成功");
        //持续轮询消费源kafka
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.valueOf(ConfUtil.getString("poll.duration"))));
                for (ConsumerRecord<String, String> record : records) {
                    //发送消息到目标kafka
                    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(target_topic, record.value()));
                    try {
                        //消息发送失败将阻塞进程，从而阻止提交偏移量
                        future.get();
                        //提交偏移量
                        consumer.commitAsync();
                    } catch (Exception e) {
                        if(!future.isCancelled()){
                            future.cancel(true);
                        }
                        e.printStackTrace();
                        consumer.close();
                        LOG.info("消息发送失败，进程退出");
                        return;
                    }
                }
            }
        }finally {
            consumer.close();
        }
    }
}
