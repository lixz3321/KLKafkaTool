package kl.transmit;

import kl.utils.ConfUtil;
import kl.utils.KafkaConf;
import kl.utils.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/24/17:42
 * @Description: 一次性转发一批数据
 */
public class BatchTransmitJob {
    public static Logger LOG = Logger.getLogger(BatchTransmitJob.class);
    public static final String hint = "请输入参数：1.模式 2.消息起始时间/起始偏移量 3.消息终止时间/终止偏移量";
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
        BatchTransmitJob.run(consumerConfName,producerConfName,args[0],args[1],args[2]);
    }

    /**
     * 作业逻辑
     */
    public static void run(String kafkaConf1,String kafkaConf2,String mod,String start,String end){
        //消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(new KafkaConf(kafkaConf1).getProperties());
        //生产者
        Producer<String, String> producer = new KafkaProducer<String,String>(new KafkaConf(kafkaConf2).getProperties());
        //读/写 topic
        String source_topic = ConfUtil.getString("kafka.source.topic");
        String target_topic = ConfUtil.getString("kafka.target.topic");
        //topic分区
        TopicPartition topicPartition = new TopicPartition(source_topic,0);
        LOG.info("输入主题："+source_topic+",输出主题："+target_topic);
        if(mod.equals("time")){//时间模式
            Long start_offset = KafkaUtil.getOffsetByDateTime(consumer, source_topic, 0, start);
            Long end_offset = KafkaUtil.getOffsetByDateTime(consumer, source_topic, 0, end);
            if(start_offset!=null && end_offset!=null){
                LOG.error("获取偏移量有误，进程退出！");
                return;
            }
            //消费者指定消费位置
            consumer.seek(topicPartition,start_offset);

            //
            long offset = 0L;

        }else if(mod.equals("offset")){//偏移量模式

        }else {
            System.out.println(hint);
            return;
        }
    }


}
