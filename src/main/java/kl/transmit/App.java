package kl.transmit;

import kl.utils.ConfUtil;
import kl.utils.KafkaConf;
import kl.utils.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/25/15:38
 * @Description:
 */
public class App {
    /**
     * 测试
     * @param args
     */
    public static void main(String[] args){
        //生产者
        Producer<String, String> producer = new KafkaProducer<String,String>(new KafkaConf("producer-local").getProperties());
        //每批次消费记录数
        int batch = 0;
        while (true){
            System.out.println("消费！！！！！！！！！！！！！！！！！！！");
            String msg = String.valueOf(new Date().getTime());
            System.out.println("消息："+msg);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("test3", msg));
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            } catch (ExecutionException e) {
                e.printStackTrace();
                break;
            }
        }
        System.out.println("退出");
    }
}
