package kl.common;

import kl.utils.ConfUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import static kl.common.TopicPartitionConsumerFactory.runThreadNum;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/25/17:10
 * @Description: 消费线程：将指定主题分区某偏移量范围的数据消费出来并写入缓存
 * 如果未指定终止偏移量，或者终止偏移量为空则持续消费
 */
public class TopicPartitionConsumer implements Runnable{

    public static Logger LOG = Logger.getLogger(TopicPartitionConsumer.class);
    /**
     * 分区号
     */
    private int innPartitionNum;
    /**
     * 消费者
     */
    private KafkaConsumer<String,String> consumer;
    /**
     * 消费起始偏移量
     */
    private Long startOffset;
    /**
     * 消费终止偏移量
     */
    private Long endOffset;
    /**
     * 最新偏移量列表
     */
    private Map<Integer, Long> newOffsets;
    /**
     * 缓存
     */
    public static BlockingQueue<String> queue = new LinkedBlockingDeque<String>();

    public void setInnPartitionNum(int partitionNum) {
        this.innPartitionNum = partitionNum;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void setStart(long startOffset) {
        this.startOffset = startOffset;
    }

    public void setEnd(long endOffset) {
        this.endOffset = endOffset;
    }

    public void setNewOffsets(Map<Integer, Long> newOffsets) {
        this.newOffsets = newOffsets;
    }

    public void setQueue(BlockingQueue<String> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        //读/写 topic
        String source_topic = ConfUtil.getString("kafka.source.topic");
        //分区
        TopicPartition topicPartition = new TopicPartition(source_topic, innPartitionNum);
        //分配消费分区
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition,startOffset);
        //如果终止偏移量为空，则持续消费,否则消费到终止偏移量即停止消费
        if(endOffset==null){
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(ConfUtil.getInt("poll.duration")));
                for (ConsumerRecord<String, String> record : records){
                    //写入缓存
                    queue.add(record.value());
                }
            }
        }else{
            long tmpOffset = 0L;
            while(tmpOffset<endOffset-1){
                //拉取一批消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(ConfUtil.getInt("poll.duration")));
                for (ConsumerRecord<String, String> record : records){
                    tmpOffset = record.offset();
                    //写入缓存
                    if(tmpOffset<=endOffset){
                        queue.add(record.value());
                    }else{
                        break;
                    }
//                    System.out.println("当前偏移量："+tmpOffset+",终止偏移量："+endOffset+",相差："+(endOffset-tmpOffset));
                }
            }
            runThreadNum--;
        }
    }
}
