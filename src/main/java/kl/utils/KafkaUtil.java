package kl.utils;

import kl.transmit.BatchTransmitJob;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/22/15:59
 * @Description:
 */
public class KafkaUtil {
    /**
     * 日志
     */
    public static Logger LOG = Logger.getLogger(KafkaUtil.class);


    /**
     * 根据时间戳获取偏移量
     *
     * @param consumer
     * @param topic
     * @param partition 分区号
     * @param datetimeStr 消息时间
     * @return 返回大于或等于给定时间戳所对应偏移量的最早偏移量
     * @throws ParseException
     */
    public static Long getOffsetByDateTime(KafkaConsumer consumer, String topic,int partition,String datetimeStr) {
        Long offset = null;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long timestamp = 0;
        try {
            timestamp = df.parse(datetimeStr).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
        Map<TopicPartition,Long> map = new HashMap();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        map.put(topicPartition,timestamp);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = null;
        try {
            offsetAndTimestampMap = consumer.offsetsForTimes(map, Duration.ofSeconds(10));
            offset = offsetAndTimestampMap.get(topicPartition).offset();
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("无法获取时间戳为["+datetimeStr+"]的偏移量，请确认当前主题在该时间戳上是否有数据");
            return null;
        }
        return offset;
    }

    /**
     * 获取某topic的分区号列表
     * @param consumer
     * @param topic
     * @return
     */
    public static int[] getPartitionNums(KafkaConsumer consumer,String topic){
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        int[] partitionNums = new int[partitionInfos.size()];
        for(int i=0;i<partitionInfos.size();i++){
            partitionNums[i] = partitionInfos.get(i).partition();
        }
        return partitionNums;
    }

    /**
     * 获取topic所有分区的最新偏移量
     * @param consumer
     * @param topic
     * @return Map<分区号,最新偏移量>
     */
    public static Map<Integer, Long> getEndOffset(KafkaConsumer consumer,String topic){
        int[] partitionNums = getPartitionNums(consumer, topic);
        List<TopicPartition> partitions = new ArrayList<>();
        for(int partitionNum:partitionNums){
            partitions.add(new TopicPartition(topic,partitionNum));
        }
        Map<TopicPartition,Long> endOffsets = consumer.endOffsets(partitions);
        Map<Integer,Long> res = new HashMap<>();
        for(TopicPartition partition:endOffsets.keySet()){
            res.put(partition.partition(),endOffsets.get(partition));
        }
        return res;
    }

    /**
     *
     * @param topic
     * @param partitionNum
     * @param mod
     * @param start
     * @param end
     * @param newOffsets 各个分区最新偏移量列表
     * @param consumer
     * @return Long[0]:起始偏移量，Long[1]终止偏移量
     */
    public static  Long[] getOffsetRange(String topic,int partitionNum,String mod,String start,String end,Map<Integer, Long> newOffsets,KafkaConsumer consumer){
        Long startOffset = null;
        Long endOffset = null;
        switch (mod){
            case "0"://用户输入时间范围
                startOffset = KafkaUtil.getOffsetByDateTime(consumer, topic, partitionNum, start);
                endOffset = KafkaUtil.getOffsetByDateTime(consumer, topic, partitionNum, end);
                if(startOffset==null && endOffset==null){
                    return null;
                }else if(startOffset!=null && endOffset==null){
                    endOffset = newOffsets.get(partitionNum);
                }
                break;
            case "1"://用户输入偏移量范围
                startOffset = Long.valueOf(start);
                endOffset = Long.valueOf(end);
                Long newOffset = newOffsets.get(partitionNum);
                if(endOffset>newOffset){
                    endOffset =newOffset;
                }
                break;
            case "2"://用户输入起始时间
                startOffset = KafkaUtil.getOffsetByDateTime(consumer, topic, partitionNum, start);
                break;
            case "3"://用户输入起始偏移量
                startOffset = Long.valueOf(start);
                break;
        }
        return new Long[]{startOffset,endOffset};
    }
}
