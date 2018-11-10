#include <time.h>
#include <fc/log/logger.hpp>
#include <cppkafka/cppkafka.h>
#include <boost/algorithm/string/join.hpp>
#include <eosio/data_plugin/producers.hpp>

namespace eosio {namespace data{

using std::vector;
using std::unique_ptr;
using namespace boost::algorithm;

struct KafkaProducer : producer<KafkaProducer> {
    void set_program_options(options_description& cli, options_description& cfg) {
        cfg.add_options()
            ("data-plugin-kafka-addr", bpo::value<vector<string> >()->composing(), "the addr of kafka endpoint, can have more than one")
            ("data-plugin-kafka-message-max-bytes", bpo::value<uint32_t>()->default_value(2000000), "the maximum bytes of one message")
            ("data-plugin-print-payload", bpo::value<bool>()->default_value(false), "if true if will print the payload with dlog")
            ("data-plugin-kafka-partition-num", bpo::value<uint32_t>()->default_value(1), "total partition num")
        ;
    }
    void initialize(const variables_map& options) {
        if (options.count("data-plugin-kafka-addr") <= 0) return;
        vector<string> addrs = options["data-plugin-kafka-addr"].as<vector<string> >(); 
        if (addrs.empty()) return;
        initialized = true;
        kafka_config = {
            {"metadata.broker.list", join(addrs, ",")},
            {"socket.keepalive.enable", true},
            {"request.required.acks", 1},
            {"compression.codec", "gzip"},
            {"message.max.bytes", options["data-plugin-kafka-message-max-bytes"].as<uint32_t>()},
        };
        print_payload = options["data-plugin-print-payload"].as<bool>();
        partition_num = options["data-plugin-kafka-partition-num"].as<uint32_t>();
        if (!initialized) return;
        kafka_producer = std::make_unique<cppkafka::Producer>(kafka_config);
        auto conf = kafka_producer->get_configuration().get_all();
        ilog ("Kafka config : ${conf}", ("conf", conf));
    }
    void startup() {
    }
    void stop() {
        if (!initialized) return;
        for (int i = 0; i < 5; i++) {
            try {
                kafka_producer->flush();
                ilog ("kafka producer flush finish");
                kafka_producer.reset();
            } catch (const std::exception& ex) {
                elog ("std Exception when close kafka producer : ${ex} try again(${i}/5", ("ex", ex.what())("i", i));
            }
        }
    }
    void produce (const string& name, const string& key, fc::variant& value) {
        if (!initialized) return;
        auto payload = fc::json::to_string(value, fc::json::legacy_generator);
        try {
            cppkafka::Buffer keyBuffer(key.data(), key.length());
            auto partition = -1;
            if (value.is_object()) {
                auto valueobj = value.get_object();
                if (valueobj.find("primary_key") != valueobj.end()) {
                    string primary_key = valueobj["primary_key"].as<string>();
                    uint64_t tmp = 0; for (int i = 0; i < primary_key.length(); i ++) tmp += primary_key[i];
                    partition = tmp % partition_num;
                }
            }
            kafka_producer->produce(cppkafka::MessageBuilder(name).partition(partition).key(keyBuffer).payload(payload));
            if (print_payload) {
                dlog ("${topic} message(size=${size}):\n${payload}", ("size", payload.length())("payload", payload));
            }
        } catch(const std::exception& ex) {
            elog ("std Exception in kafka_producer when produce [ex=${ex}] [topic=${topic}] [key=${key}] [payload=${payload}]",
                ("ex", ex.what())("topic", name)("key", key)("payload", payload));
        }
    }
    
    unique_ptr<cppkafka::Producer> kafka_producer;
    cppkafka::Configuration kafka_config;
    bool print_payload;
    bool initialized = false;
    uint32_t partition_num;
};
static auto _kafka_producer = eosio::data::producers().register_producer<KafkaProducer>();

}}
