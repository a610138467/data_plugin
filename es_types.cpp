#include <queue>
#include <string>
#include <vector>
#include <chrono>
#include <fc/io/json.hpp>
#include <eosio/chain/asset.hpp>
#include <boost/lexical_cast.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/data_plugin/types.hpp>

namespace eosio{ namespace data{ namespace es{

using std::queue;
using std::string;
using std::vector;
using namespace chain;
using key_values = abstract_type::key_values;

struct BlockInfo : type<BlockInfo> {
    key_values build(const block_state_ptr& bsp, const fc::mutable_variant_object& obj) {
        key_values res;
        auto resobj = fc::mutable_variant_object
            ("create_time", fc::time_point::now())
            ("primary_key", string(obj["id"].as<block_id_type>()))
            ("block_id_askey", obj["id"])
            ("previous", obj["header"].get_object()["previous"])
            ("block_num_askey",obj["block_num"])
            ("block_time", obj["header"].get_object()["timestamp"])
            ("producer", obj["header"].get_object()["producer"])
            ("transaction_mroot", obj["header"].get_object()["transaction_mroot"])
            ("action_mroot", obj["header"].get_object()["action_mroot"])
            ("block_signing_key", obj["block_signing_key"])
            ("producer_signature", obj["header"].get_object()["producer_signature"])
        ;
        vector <transaction_id_type> trxids;
        auto block_time = obj["header"].get_object()["timestamp"].as<block_timestamp_type>();
        resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
        for (auto trxobj : obj["block"].get_object()["transactions"].get_array()) {
            auto trx = trxobj.as<transaction_receipt>();
            if (trx.trx.contains<transaction_id_type>())
                trxids.push_back(trx.trx.get<transaction_id_type>());
            else
                trxids.push_back(trx.trx.get<packed_transaction>().id());
        }
        if (obj["irreversible"].as<bool>()) {
            resobj.set("irreversible", obj["irreversible"]);
        }
        resobj.set("trxs", fc::json::to_string(trxids, fc::json::legacy_generator));
        resobj.set("trxs_num", static_cast<uint32_t>(trxids.size()));
        string key = string(obj["id"].as<block_id_type>()); 
        key += obj["irreversible"].as<bool>() ? 'T' : 'F';
        res.push_back({key, resobj});
        return res;
    }
};
static auto _block_info = types().register_type<BlockInfo>();

struct TransactionInfo : type<TransactionInfo> {
    key_values build(const block_state_ptr& bst, const fc::mutable_variant_object& obj) {
        key_values res;
        for (auto trxobj : obj["block"].get_object()["transactions"].get_array()) {
            auto trx = trxobj.as<transaction_receipt>();
            auto resobj = fc::mutable_variant_object
                ("create_time", fc::time_point::now())
                ("block_id_askey", obj["id"])
                ("block_num_askey",obj["block_num"])
                ("block_time", obj["header"].get_object()["timestamp"])
                ("producer", obj["header"].get_object()["producer"])
                ("cpu_usage_us", trxobj["cpu_usage_us"])
                ("net_usage_words", trxobj["net_usage_words"])
            ;
            auto block_time = obj["header"].get_object()["timestamp"].as<block_timestamp_type>();
            resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
            if (obj["irreversible"].as<bool>()) {
                resobj.set("irreversible", obj["irreversible"]);
            }
            if (trx.trx.contains<transaction_id_type>()) {
                auto trxid = string(trx.trx.get<transaction_id_type>());
                resobj.set("primary_key", trxid);
                resobj.set("transaction_id_askey", trxid);
            }
            else {
                auto signed_trx = trx.trx.get<packed_transaction>().get_signed_transaction(); 
                auto trxid = string(signed_trx.id());
                resobj.set("primary_key", trxid);
                resobj.set("transaction_id_askey", trxid);
                resobj.set("context_free_actions_num", static_cast<uint32_t>(signed_trx.context_free_actions.size()));
                resobj.set("actions_num", static_cast<uint32_t>(signed_trx.actions.size()));
            }
            string key = resobj["primary_key"].as<string>();
            key += "FB";
            res.push_back({key, resobj});
        }
        return res;
    }
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        auto resobj = fc::mutable_variant_object
            ("create_time", fc::time_point::now())
            ("primary_key", string(obj["id"].as<transaction_id_type>()))
        ;
        if (obj.find("producer_block_id") != obj.end())
            resobj.set("block_id", obj["producer_block_id"]);
        if (obj.find("block_num") != obj.end())
            resobj.set("block_num",obj["block_num"]);
        if (obj.find("block_time") != obj.end()) {
            resobj.set("block_time", obj["block_time"]);
            auto block_time = obj["block_time"].as<block_timestamp_type>();
            resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
        }
        string key = string(obj["id"].as<transaction_id_type>());
        key += "FT";
        res.push_back({key, resobj});
        return res;
    }
};
static auto _transaction_info = types().register_type<TransactionInfo>();

struct ActionInfo : type<ActionInfo> {
    key_values build(const block_state_ptr& bsp, const fc::mutable_variant_object& obj) {
        key_values res;
        auto block_id = obj["id"].as<block_id_type>();
        auto block_num= obj["block_num"].as<uint32_t>();
        auto block_time=obj["header"].get_object()["timestamp"].as<block_timestamp_type>(); 
        auto timestamp =fc::time_point(block_time).sec_since_epoch();
        for (auto trxobj : obj["block"].get_object()["transactions"].get_array()) {
            auto trx = trxobj.as<transaction_receipt>();
            if (trx.trx.contains<transaction_id_type>()) continue;
            auto signed_trx = trx.trx.get<packed_transaction>().get_signed_transaction();
            auto trxid = signed_trx.id();
            int index = 0;
            const char* index_ptr = (const char*)(&index);
            for (auto action : signed_trx.context_free_actions) {
                auto resobj = fc::mutable_variant_object
                    ("create_time", fc::time_point::now())
                    ("block_id", block_id)
                    ("block_num", block_num)
                    ("block_time", block_time)
                    ("timestamp", timestamp)
                    ("trxid", trxid)
                    ("account_askey", action.account)
                    ("name_askey", action.name)
                ;
                string key = string(trxid);
                for (int i = 0; i < sizeof(index); i++) {
                    char tmp[8];sprintf(tmp, "%02x", index_ptr[i]);
                    key += tmp;
                }
                res.push_back({key, resobj});
                index ++;
            }
            for (auto action : signed_trx.actions) {
                auto resobj = fc::mutable_variant_object
                    ("create_time", fc::time_point::now())
                    ("block_id", block_id)
                    ("block_num", block_num)
                    ("block_time", block_time)
                    ("timestamp", timestamp)
                    ("trxid", trxid)
                    ("account_askey", action.account)
                    ("name_askey", action.name)
                ;
                string key = string(trxid);
                for (int i = 0; i < sizeof(index); i++) {
                    char tmp[8];sprintf(tmp, "%02x", index_ptr[i]);
                    key += tmp;
                }
                res.push_back({key, resobj});
                index ++;
            }
        }
        return res;
    }
};
static auto _action_info = types().register_type<ActionInfo>();

struct ActionTrace : type<ActionTrace> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        for (auto var : obj["total_action_traces"].get_array()) {
            auto trace = var.get_object();
            auto resobj = fc::mutable_variant_object
                ("create_time", fc::time_point::now())
                ("transaction_id_askey", trace["trx_id"])
                ("global_sequence", trace["receipt"].get_object()["global_sequence"])
                ("cpu_usage", trace["cpu_usage"])
                ("total_cpu_usage", trace["total_cpu_usage"])
                ("receiver_askey", trace["receipt"].get_object()["receiver"])
                ("account_askey", trace["act"].get_object()["account"])
                ("name_askey", trace["act"].get_object()["name"])
                ("data", fc::json::to_string(trace["act"].get_object()["data"], fc::json::legacy_generator))
            ;
            if (trace.find("block_time") != trace.end()) {
                auto block_time = obj["block_time"].as<block_timestamp_type>();
                resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
                resobj.set("block_time", trace["block_time"]);
            }
            if (trace.find("producer_block_id") != trace.end())
                resobj.set("block_id_askey", trace["producer_block_id"]);
            if (trace.find("block_num") != trace.end())
                resobj.set("block_num", trace["block_num"]);
            vector<uint64_t> global_sequences;
            for (auto inline_trace : trace["inline_traces"].get_array()) {
                global_sequences.push_back(inline_trace.get_object()["receipt"].get_object()["global_sequence"].as<uint64_t>()); 
            }
            resobj.set("inline_traces", fc::json::to_string(global_sequences, fc::json::legacy_generator));
            resobj.set("inline_traces_num", static_cast<uint32_t>(global_sequences.size()));
            string key = build_action_key(trace);
            resobj.set("primary_key", key);
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _action_trace = types().register_type<ActionTrace>();
 
struct TransferLog : type<TransferLog> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        for (auto var : obj["total_action_traces"].get_array()) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<action_name>());
            if (! (account == "eosio.token" && name == "transfer")) return res;

            auto resobj = fc::mutable_variant_object
                ("create_time", fc::time_point::now())
                ("transaction_id_askey", trace["trx_id"])
                ("global_sequence", trace["receipt"].get_object()["global_sequence"])
            ;
            if (trace.find("block_time") != trace.end()) {
                auto block_time = obj["block_time"].as<block_timestamp_type>();
                resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
                resobj.set("block_time", trace["block_time"]);
            }
            if (trace.find("producer_block_id") != trace.end())
                resobj.set("block_id", trace["producer_block_id"]);
            if (trace.find("block_num") != trace.end())
                resobj.set("block_num", trace["block_num"]);
            auto data = trace["act"].get_object()["data"].get_object();
            resobj.set("from_askey", data["from"]);
            resobj.set("to_askey", data["to"]);
            resobj.set("memo", data["memo"]);
            auto quantity = data["quantity"].as<asset>();
            resobj.set("amount", quantity.to_real());
            resobj.set("symbol_askey", quantity.symbol_name());
            string key = build_action_key(trace);
            resobj.set("primary_key", key);
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _transfer_log = types().register_type<TransferLog>();

struct SetContractLog : type<SetContractLog> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        for (auto var : obj["total_action_traces"].get_array()) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<action_name>());
            if (! (account == "eosio" && (name == "setcode" || name == "setabi"))) return res;
            auto resobj = fc::mutable_variant_object
                ("create_time", fc::time_point::now())
                ("transaction_id_askey", trace["trx_id"])
                ("global_sequence", trace["receipt"].get_object()["global_sequence"])
                ("name", name)
            ;
            if (trace.find("block_time") != trace.end()) {
                auto block_time = obj["block_time"].as<block_timestamp_type>();
                resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
                resobj.set("block_time", trace["block_time"]);
            }
            if (trace.find("producer_block_id") != trace.end())
                resobj.set("block_id", trace["producer_block_id"]);
            if (trace.find("block_num") != trace.end())
                resobj.set("block_num", trace["block_num"]);
            auto data = trace["act"].get_object()["data"].get_object();
            resobj.set("account_askey", data["account"]);
            if (data.find("vmtype") != data.end())
                resobj.set("vmtype", data["vmtype"]);
            if (data.find("vmversion") != data.end())
                resobj.set("vmversion", data["vmversion"]);
            if (data.find("code") != data.end())
                resobj.set("code", data["code"]);
            else if (data.find("abi") != data.end())
                resobj.set("abi", data["abi"]);
            string key = build_action_key(trace);
            resobj.set("primary_key", key);
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _set_contract_log = types().register_type<SetContractLog>();
    
struct TokenInfo : type<TokenInfo> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        for (auto var : obj["total_action_traces"].get_array()) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<action_name>());
            if (! (account == "eosio.token" && name == "create" )) return res;
            auto resobj = fc::mutable_variant_object
                ("create_time", fc::time_point::now())
                ("transaction_id_askey", trace["trx_id"])
                ("global_sequence", trace["receipt"].get_object()["global_sequence"])
            ;
            if (trace.find("block_time") != trace.end()) {
                auto block_time = obj["block_time"].as<block_timestamp_type>();
                resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
                resobj.set("block_time", trace["block_time"]);
            }
            if (trace.find("producer_block_id") != trace.end())
                resobj.set("block_id", trace["producer_block_id"]);
            if (trace.find("block_num") != trace.end())
                resobj.set("block_num", trace["block_num"]);
            auto data = trace["act"].get_object()["data"].get_object();
            resobj.set("issuer_askey", data["issuer"]);
            auto maximum_supply = data["maximum_supply"].as<asset>();
            resobj.set("total_amount", maximum_supply.to_real());
            resobj.set("symbol_askey", maximum_supply.symbol_name());
            string key = build_action_key(trace);
            resobj.set("primary_key", key);
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _token_info = types().register_type<TokenInfo>();

struct IssueLog : type<IssueLog> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        for (auto var : obj["total_action_traces"].get_array()) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<action_name>());
            if (! (account == "eosio.token" && name == "issue" )) return res;
            auto resobj = fc::mutable_variant_object
                ("create_time", fc::time_point::now())
                ("transaction_id_askey", trace["trx_id"])
                ("global_sequence", trace["receipt"].get_object()["global_sequence"])
            ;
            if (trace.find("block_time") != trace.end()) {
                auto block_time = obj["block_time"].as<block_timestamp_type>();
                resobj.set("timestamp", fc::time_point(block_time).sec_since_epoch());
                resobj.set("block_time", trace["block_time"]);
            }
            if (trace.find("producer_block_id") != trace.end())
                resobj.set("block_id", trace["producer_block_id"]);
            if (trace.find("block_num") != trace.end())
                resobj.set("block_num", trace["block_num"]);
            auto data = trace["act"].get_object()["data"].get_object();
            resobj.set("to_askey", data["to"]);
            auto quantity = data["quantity"].as<asset>();
            resobj.set("amount", quantity.to_real());
            resobj.set("symbol", quantity.symbol_name());
            resobj.set("memo", data["memo"]);
            string key = build_action_key(trace);
            resobj.set("primary_key", key);
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _issue_log = eosio::data::types().register_type<IssueLog>();

}}} //eosio::data::es
