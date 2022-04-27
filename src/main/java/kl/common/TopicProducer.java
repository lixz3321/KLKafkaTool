package kl.common;

import kl.utils.KafkaConf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/27/9:39
 * @Description: 测试
 */
public class TopicProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //生产者
        Producer<String, String> producer = new KafkaProducer<String,String>(new KafkaConf("producer-hdp-dev").getProperties());
        char[] arr = {'a','b','c','d','e','f','g','h','i','j','k','l','m','n'};
        for(int i=0;i<9999;i++){
            for(char c:arr){
                String msg =  ""+c+i;
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>("test5", msg));
//                future.get();
                System.out.println("生产消息："+msg);
                Thread.sleep(10);
            }
        }
        producer.close();
    }
}
