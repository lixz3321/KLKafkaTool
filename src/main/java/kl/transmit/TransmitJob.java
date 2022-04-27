package kl.transmit;

import kl.utils.ConfUtil;
import kl.utils.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/22/16:41
 * @Description: kafka消息跨集群转发
 *  *该程序自动获取起始偏移量，如需指定起始偏移量可通过手动执行kafka-consumer-groups，将其
 *  分组的偏移量修改成期望的偏移量值
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
        //至少多少条消息提交一次偏移量
        int maxCommitRowNum = ConfUtil.getInt("max.commit.row.num");
        //每个偏移量提交间隔内的已发送行数
        int sendedRowNum = 0;
        //持续轮询消费源kafka
        Future<RecordMetadata> future=null;
        try {
            while (true) {
                //拉取一批消息
                ConsumerRecords<String, String> records = consumer.poll(ConfUtil.getInt("poll.duration"));
                if(records.isEmpty()){
                    continue;
                }
                //将这批消息逐条发送到目标主题
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("当前一次拉取条数："+records.count()+"，发送消息："+record.value()+",偏移量："+record.offset());
                    future = producer.send(new ProducerRecord<>(target_topic, record.value()));
                    sendedRowNum++;
                }
                //判断已发送条数满足偏移量提交条件
                if(sendedRowNum>=maxCommitRowNum){
                    //确认这批消息的最后一条记录是否发送成功，如果broker异常，这里将会阻塞
                    future.get();
                    //这批消息最后一条记录发送成功即可提交偏移量
                    consumer.commitAsync();
//                    System.out.println("------------------------------ 提交偏移量成功！--------------批次条数："+sendedRowNum+"------------");
                    sendedRowNum=0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("转发作业遇到异常，即将退出！");
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
