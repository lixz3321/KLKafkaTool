package kl.utils;

import kl.transmit.BatchTransmitJob;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/22/15:59
 * @Description:
 */
public class KafkaUtil {
    public static Logger LOG = Logger.getLogger(KafkaUtil.class);


    /**
     * 根据时间戳获取偏移量
     * @param consumer
     * @param topic
     * @param partition 分区号
     * @param datetimeStr 消息时间
     * @return
     * @throws ParseException
     */
    public static Long getOffsetByDateTime(KafkaConsumer consumer, String topic,int partition,String datetimeStr) {
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
        Map<TopicPartition, OffsetAndTimestamp> offset = null;
        try {
            offset = consumer.offsetsForTimes(map, Duration.ofSeconds(10));
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("无法获取时间戳为["+datetimeStr+"]的偏移量，请确认当前主题在该时间戳上是否有数据");
            return null;
        }
        return  offset.get(topicPartition).offset();
    }
}
