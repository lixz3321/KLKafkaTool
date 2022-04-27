#!/usr/bin/env bash
##################################################################
#
# kafka数据转发作业启动脚本
# 作者：李新兆
# 描述：跨集群实时转发kafka消息
# 如果使用新的消费组，则从最新偏移量处开始消费
# 配置文件：conf.properties
##################################################################
dir=$(cd $(dirname $0);pwd)
nohup java -Djava.security.auth.login.config=$dir/jaas_v2.0.conf -cp .:KLKafkaTool-1.0.jar:$dir/lib/kafka-clients-2.0.0.jar:$dir/lib/lz4-java-1.4.1.jar:$dir/lib/slf4j-api-1.7.25.jar:$dir/lib/snappy-java-1.1.7.1.jar:$dir/lib/log4j-1.2.17.jar:$dir/lib/slf4j-log4j12-1.6.1.jar kl.transmit.TransmitJob >/dev/null 2>&1 &