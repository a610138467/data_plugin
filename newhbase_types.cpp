#include <queue>
#include <string>
#include <fc/io/json.hpp>
#include <boost/lexical_cast.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/data_plugin/types.hpp>

namespace eosio{ namespace data{ namespace newhbase{

using std::string;
using std::queue;
using eosio::data::type;
using fc::time_point_sec;
using namespace chain;
using key_values = abstract_type::key_values;

struct BlockState : type<BlockState>{
    key_values build(const block_state_ptr& bsp, const mutable_variant_object& obj) {
        key_values res;
        if (!obj["irreversible"]) {
            string block_time = obj["header"].get_object()["timestamp"].as<string>();
            size_t replace_pos = block_time.find("-");
            while (replace_pos != string::npos) {
                block_time.replace(replace_pos, 1, "");
                replace_pos = block_time.find("-");
            }
            size_t pos = block_time.find('T');
            if (pos != string::npos) {
                if (block_time.length() > pos) {
                    block_time = block_time.substr(0, pos - 2);
                }
            }
            //解析出所有的transactionID
            std::vector<string> trx_ids;
            for (auto trx : bsp->block->transactions) {
                if (trx.trx.contains<transaction_id_type>()) {
                    trx_ids.push_back(trx.trx.get<transaction_id_type>());
                } else if (trx.trx.contains<packed_transaction>()) {
                    trx_ids.push_back(trx.trx.get<packed_transaction>().id());
                }
            }
            auto resobj = fc::mutable_variant_object
                ("table_suffix", block_time)
                ("primary_key", bsp->id)

                ("origin:json", fc::json::to_string(obj, fc::json::legacy_generator))

                ("info:block_id", bsp->id)
                ("info:block_num",bsp->block_num)
                ("info:block_time",bsp->header.timestamp)
                ("info:trxs", fc::json::to_string(trx_ids, fc::json::legacy_generator))
            ;
            string key = string(obj["id"].as<block_id_type>());
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _block_state = eosio::data::types().register_type<BlockState>();

struct TransactionTrace : type<TransactionTrace>{
    key_values build(const transaction_trace_ptr& ttp, const mutable_variant_object& obj) {
        key_values res;
        string block_time = obj["block_time"].as<string>();
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        if (pos != string::npos) {
            if (block_time.length() > pos) {
                block_time = block_time.substr(0, pos - 2);
            }
        }
        char primary_key[256] = {0};
        sprintf(primary_key, "%08x %s", ttp->block_num, string(ttp->id).c_str());
        auto resobj = fc::mutable_variant_object
            ("primary_key", primary_key)
            ("table_suffix", block_time)
            ("origin:json", fc::json::to_string(obj, fc::json::legacy_generator))

            ("info:block_num", ttp->block_num)
            ("info:block_time",ttp->block_time)
            ("info:action_num",ttp->action_traces.size())
        ;
        string key = string(obj["id"].as<transaction_id_type>());
        res.push_back({key, resobj});
        return res;
    }
};
static auto _transaction_trace = eosio::data::types().register_type<TransactionTrace>();
    
}}} //eosio::data::hbase
