package kl.transmit;

import kl.common.TopicPartitionConsumer;
import kl.common.TopicPartitionConsumerFactory;
import kl.utils.ConfUtil;
import kl.utils.KafkaConf;
import kl.utils.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static kl.common.TopicPartitionConsumerFactory.runThreadNum;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/24/17:42
 * @Description: 一次性转发一批数据
 * 转发时不再提交偏移量，如需提交偏移量可在消费者配置中设置自动提交偏移量
 */
public class BatchTransmitJob {
    public static Logger LOG = Logger.getLogger(BatchTransmitJob.class);
    public static final String hint = "请输入参数：\n1.模式[0:转发某时间范围的一批数据,1:转发某偏移量范围的一批数据,2:指定起始时间持续转发,3:指定起始偏移量持续转发] " +
            "\n2.消息起始时间/起始偏移量 " +
            "\n3.消息终止时间/终止偏移量";
    //总共转发行数
    public static long row_num = 0L;
    /**
     * 执行入口
     * @param args
     */
    public static void main(String[] args){
        if(args.length!=3){
            System.out.println(hint);
            return;
        }
        String consumerConfName = ConfUtil.getString("kafka.consumer.conf");
        String producerConfName = ConfUtil.getString("kafka.producer.conf");
        LOG.info("当前使用的kafka消费者配置："+consumerConfName+",生产者配置："+producerConfName);
        BatchTransmitJob.doJob(consumerConfName,producerConfName,args[0],args[1],args[2]);
    }

    /**
     * 作业逻辑
     */
    public static void doJob(String kafkaConf1,String kafkaConf2,String mod,String start,String end){
        //读/写 topic
        String source_topic = ConfUtil.getString("kafka.source.topic");
        String target_topic = ConfUtil.getString("kafka.target.topic");
        //临时消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(new KafkaConf(kafkaConf1).getProperties());
        //获取主题分区号列表
        int[] partitionNums = KafkaUtil.getPartitionNums(consumer, source_topic);
        //关闭临时消费者
        consumer.close();
        //开始消费
        TopicPartitionConsumerFactory factory = new TopicPartitionConsumerFactory(kafkaConf1,mod,start,end);
        factory.startConsumer();
        /*
        读取缓存中的数据并发送到目标topic
         */
        //阻塞，直到所有消费进程创建完成
        while(partitionNums.length>runThreadNum){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOG.info("消费线程初始化···");
        }
        Producer<String, String> producer = new KafkaProducer<String,String>(new KafkaConf(kafkaConf2).getProperties());
        LOG.info("开始转发数据···");
        while (runThreadNum>0 || !TopicPartitionConsumer.queue.isEmpty()){
            String value = TopicPartitionConsumer.queue.poll();
            if(value!=null){
                producer.send(new ProducerRecord<>(target_topic, value));
                System.out.println("发送消息："+value);
                row_num++;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //刷数据
        producer.flush();
        LOG.info("数据转发完成，已发送数据："+row_num+"条");
    }
}
