# Kafka Plugin

## 说明

提供将eosio的区块链数据导入到kafka的功能。支持kafka集群。

## 依赖

**libkafka**

## 编译

### 安装libkafka

```
git clone https://github.com/edenhill/librdkafka
cd librdkafka
cmake -H. -B_cmake_build
cmake -DRDKAFKA_BUILD_STATIC=1 --build _cmake_build
cd _cmake_build
make install
```

### 下载kafka-plugin代码到eosio的plugs目录

```
cd plugins
git clone https://github.com/a610138467/kafka_plugin.gitz
cd ..
```

### 初始化环境

```
./plugins/kafka_plugin/init.sh
```

### 编译eosio的代码

```
./eosio_build.sh
```

## 使用

```
./build/programs/nodeos/nodeos --plugin eosio::kafka_plugin --kafka-enable true --kafka-broker-list ${kafkanode1},${kafkanode2}......
```

## 支持参数

参数名称 | 参数说明
------ | --------
kafka-enable | 是否启动
kafka-broker-list | kafka地址，多个地址用","分割
kafka-block-topic | 存储blocks信息的topic名称
kafka-transaction-topic | 存储transaction信息的topic名称
kafka-transaction-trace-topic | 存储transaction-trace信息的topic名称
kafka-action-topic | 存储action信息的topic名称
kafka-batch-num-messages | 缓存消息数目
kafka-queue-buffering-max-ms | 缓存消息最大数
kafka-compression-codec | 消息压缩格式
kafka-request-required-acks | 是否确认
kafka-message-send-max-retries | 重试次数
kafka-start-block-num | 开始数
kafka-statistics-interval-ms | 静态分析的时间间隔
kafka-fixed-partition | partition值