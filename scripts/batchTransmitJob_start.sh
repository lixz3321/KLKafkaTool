#!/usr/bin/env bash
##################################################################
#
# kafka数据补录作业启动脚本
# 作者：李新兆
# 描述：跨集群转发一批kafka消息（转发时不再支持消息转发确认并提交偏移量，如需提交偏移量可在消费者配置中设置自动提交偏移量）
# 参数：1、模式[0:转发某时间范围的一批数据,1:转发某偏移量范围的一批数据,2:指定起始时间持续转发,3:指定起始偏移量持续转发]
#       2、消息起始时间/起始偏移量
#       3、消息终止时间/终止偏移量
# 配置文件：conf.properties
##################################################################
dir=$(cd $(dirname $0);pwd)
#模式
mod=0

nohup java -Djava.security.auth.login.config=$dir/jaas_v2.0.conf -cp .:KLKafkaTool-1.0.jar:$dir/lib/kafka-clients-2.0.0.jar:$dir/lib/lz4-java-1.4.1.jar:$dir/lib/slf4j-api-1.7.25.jar:$dir/lib/snappy-java-1.1.7.1.jar:$dir/lib/log4j-1.2.17.jar:$dir/lib/slf4j-log4j12-1.6.1.jar kl.transmit.BatchTransmitJob ${mod} "2022-04-25 10:00:00" "2022-04-25 11:00:00" &