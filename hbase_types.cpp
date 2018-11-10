#include <queue>
#include <string>
#include <fc/io/json.hpp>
#include <boost/lexical_cast.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/data_plugin/types.hpp>

namespace eosio{ namespace data{ namespace hbase{

using std::string;
using std::queue;
using eosio::data::type;
using fc::time_point_sec;
using namespace chain;
using key_values = abstract_type::key_values;

struct IrreversibleBlockState : type<IrreversibleBlockState>{
    key_values build(const block_state_ptr& bsp, const mutable_variant_object& obj) {
        key_values res;
        if (obj["irreversible"].as<bool>()) {
            auto resobj = fc::mutable_variant_object
                ("primary_key", string(obj["id"].as<block_id_type>()))
                ("json", fc::json::to_string(obj, fc::json::legacy_generator))
                ("bytes",fc::raw::pack(bsp));
            auto block_time = obj["header"].get_object()["timestamp"].as<block_timestamp_type>();
            resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
            string key = string(obj["id"].as<block_id_type>());
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _irreversible_block_state = eosio::data::types().register_type<IrreversibleBlockState>();

struct ReversibleBlockState : type<ReversibleBlockState>{
    key_values build(const block_state_ptr& bsp, const mutable_variant_object& obj) {
        key_values res;
        if (!obj["irreversible"]) {
            auto resobj = fc::mutable_variant_object
                ("primary_key", string(obj["id"].as<block_id_type>()))
                ("json", fc::json::to_string(obj, fc::json::legacy_generator))
                ("bytes",fc::raw::pack(bsp));
            auto block_time = obj["header"].get_object()["timestamp"].as<block_timestamp_type>();
            resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
            string key = string(obj["id"].as<block_id_type>());
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _reversible_block_state = eosio::data::types().register_type<ReversibleBlockState>();

struct TransactionTrace : type<TransactionTrace>{
    key_values build(const transaction_trace_ptr& ttp, const mutable_variant_object& obj) {
        key_values res;
        auto resobj = fc::mutable_variant_object
            ("primary_key", string(obj["id"].as<transaction_id_type>()))
            ("json", fc::json::to_string(obj, fc::json::legacy_generator))
            ("bytes",fc::raw::pack(ttp));
        string key = string(obj["id"].as<transaction_id_type>());
        res.push_back({key, obj});
        return res;
    }
};
static auto _transaction_trace = eosio::data::types().register_type<TransactionTrace>();
    
struct TransactionMetadata : type<TransactionMetadata>{
    key_values build(const transaction_metadata_ptr& tmp, const mutable_variant_object& obj) {
        key_values res;
        auto resobj = fc::mutable_variant_object
            ("primary_key", string(tmp->id))
            ("json", fc::json::to_string(obj, fc::json::legacy_generator))
            ("bytes",fc::raw::pack(tmp));
        string key = string(tmp->id);
        res.push_back({key, obj});
        return res;
    }
};
static auto _transaction_metadata = eosio::data::types().register_type<TransactionMetadata>(); 

struct ActionTrace : type<ActionTrace>{
    key_values build(const transaction_trace_ptr& ttp, const mutable_variant_object& obj) {
        key_values res;
        for (auto trace : obj["total_action_traces"].get_array()) {
            string key = build_action_key(trace.get_object());
            auto resobj = fc::mutable_variant_object
                ("primary_key", key)
                ("json", fc::json::to_string(trace, fc::json::legacy_generator))
                ("bytes",fc::raw::pack(trace))
            ;
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _action_trace = eosio::data::types().register_type<ActionTrace>();

}}} //eosio::data::hbase
