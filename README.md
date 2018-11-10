# Data Plugin

## 说明

data_plugin 是 eosio的一个插件，本插件希望能够提供eosio中区块数据和运行数据的实时处理。用户可以定义自己的数据结构(type)来导出想要的数据，也可以定义自己的生产者(producer)来将数据导出到指定的位置。

启动eosio节点后可以配置启动特定的type和producer满足自己的需求

### 定义自己的数据结构

1. 在 data\_plugin 目录下定义 xxx\_types.cpp 并在其中定义自己的数据结构。目前定义了es\_types.cpp和hbase\_types.cpp分别定义了区块的关键信息和全部信息，用于存储到es和hbase上。

2. 在eosio::data命名空间下定义结构体类型xxx，并继承自 type<xxx>如es_types.cpp中blockinfo的定义

```
struct BlockInfo : type<BlockInfo> {
    ......
};
``` 

3. 实现虚函数定义获取数据的逻辑，type基类实现了三个虚函数

```
    virtual key_values build(const block_state_ptr& bsp, const fc::mutable_variant_object& o) {
        return key_values();
    }
    virtual key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& o) {
        return key_values();
    }
    virtual key_values build(const transaction_metadata_ptr& tmp, const fc::mutable_variant_object& o) {
        return key_values();
    }
```
他们是目前eosio本身回调会传回的三种类型，用户可自定义需要的数据来源。如对于blockinfo结构只需要block_state_ptr结构，所以只需要实现第一个虚函数即可。参数2为参数1经过abi_serialize之后的variant数据。

4. 实现逻辑，返回数据
build函数返回一个 vector<pair<string, fc::variant>> 类型的数据，每条数据由 {key, value}组成，其中key是一个字符串由用户定义的，一般定义为全局唯一，如block_id。具体需求和producer相关，如果确定producer无特殊需求可忽略。value是一个fc::variant结构。这两个值会传给生效的producer，由其决定如何处理。

5. 注册结构体

目前结构体只是被定义了海没有被全局可见，需要定义类似如下语句来注册到全局map中

```
static auto _block_info = types().register_type<BlockInfo>();
```

### 定义自己的生产者

1. 在 data_plugin目录下定义 xxx\_producer.cpp文件用于实现自己的producer。目前实现了kafka\_producer、file\_producer、http\_producer分别用于把数据导入到kafka中、文件中和发送http请求通知。
2. 在eosio::data命名空间下定义结构体xxx并继承自 producer<xxx>。如kafak_producer的定义如下

```
struct KafkaProducer : producer<KafkaProducer> {
	...
}
```
3. 定义基类的五个纯虚函数用于实现具体逻辑。目前基类的虚函数如下：
```
    virtual void produce (const string& name, const string& key, fc::variant& value) = 0;
    virtual void set_program_options(options_description& cli, options_description& cfg) = 0;
    virtual void initialize(const variables_map& options) = 0;
    virtual void startup() = 0;
    virtual void stop() = 0;
```
分别在插件的set_program_options、initialize、startup、stop和有实际数据要生产时被调用。用户根据实际需求具体实现。
4. 用如下方式注册插件到全局

```
static auto _kafka_producer = eosio::data::producers().register_producer<KafkaProducer>();
```

## 启动type和producer

在配置文件中使用如下方式启用任意多个type和producer
```
data-plugin-struct = eosio::data::hbase::IrreversibleBlockState
data-plugin-file-producer-file-name = data_plugin/data
```
同时需要在配置文件中设置各个producer需要的变量

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

### 下载data-plugin代码到eosio的plugs目录

```
cd plugins
git clone https://github.com/a610138467/kafka_plugin.gitz
cd ..
```

### 初始化环境

```
./plugins/data_plugin/init.sh
```

### 编译eosio的代码

```
./eosio_build.sh
```