package kl.common;

import kl.utils.ConfUtil;
import kl.utils.KafkaConf;
import kl.utils.KafkaUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static kl.common.TopicPartitionConsumer.queue;


/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/26/9:17
 * @Description:
 */
public class TopicPartitionConsumerFactory {
    public static Logger LOG = Logger.getLogger(TopicPartitionConsumerFactory.class);
    /*
    消费者配置
     */
    private String consumerConf;
    /*
    消费模式：0：从现有分组偏移量处开始消费，1：从指定开始偏移量处消费，3：消费指定时间范围的一批数据，4：消费指定偏移量范围的一批数据
     */
    private String mod;
    /*
    消费起始时间/偏移量
     */
    private String start;
    /*
    消费终止时间/偏移量
     */
    private String end;
    //当前正在运行的消费线程计数
    public static int runThreadNum = 0;

    /*
    构造
     */
    public TopicPartitionConsumerFactory(String consumerConf, String mod, String start, String end){
        this.consumerConf = consumerConf;
        this.mod = mod;
        this.start = start;
        this.end = end;
    }

    /**
     * 创建消费者线程
     * @return
     */
    public List<TopicPartitionConsumer> createConsumer(){
        List<TopicPartitionConsumer> consumers = new ArrayList<>();
        //读/写 topic
        String source_topic = ConfUtil.getString("kafka.source.topic");
        //临时消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(new KafkaConf(consumerConf).getProperties());
        //获取主题分区号列表
        int[] partitionNums = KafkaUtil.getPartitionNums(consumer, source_topic);
        //获取topic所有分区的最新偏移量
        Map<Integer, Long> newOffsets = KafkaUtil.getEndOffset(consumer, source_topic);
        //关闭临时消费者
        consumer.close();
        consumer = null;
        //为每个分区创建消费线程
        for(int partitionNum:partitionNums){
            //创建消费者
            consumer = new KafkaConsumer<String, String>(new KafkaConf(consumerConf).getProperties());
            //获取偏移量范围
            Long[] offsetRange = KafkaUtil.getOffsetRange(source_topic,partitionNum,mod,start,end,newOffsets,consumer);
            if(offsetRange==null){
                LOG.error("获取偏移量范围失败！");
                return null;
            }
            //消费线程
            TopicPartitionConsumer runner = new TopicPartitionConsumer();

            runner.setConsumer(consumer);
            runner.setInnPartitionNum(partitionNum);
            runner.setStart(offsetRange[0]);
            runner.setEnd(offsetRange[1]);
            runner.setNewOffsets(newOffsets);
            runner.setQueue(queue);
            consumers.add(runner);
        }
        return consumers;
    }

    /**
     * 启动消费进程
     */
    public void startConsumer(){
        List<TopicPartitionConsumer> consumers = createConsumer();
        if(consumers==null){
            LOG.error("无法创建和启动消费线程");
            return;
        }
        int num = 0;
        for(TopicPartitionConsumer consumer:consumers){
            new Thread(consumer,"consumer-" + num++).start();
            runThreadNum++;
        }
    }
}
