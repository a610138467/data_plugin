#include <queue>
#include <string>
#include <vector>
#include <chrono>
#include <algorithm>
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

template<class C, class T>
inline auto contains(const C& v, const T& x) -> decltype(end(v), true)
{
    return end(v) != std::find(begin(v), end(v), x);
}

struct BlockInfo : type<BlockInfo> {
    key_values build(const block_state_ptr& bsp, const fc::mutable_variant_object& obj) {
        key_values res;
        fc::mutable_variant_object resobj;
        if (!obj["irreversible"].as<bool>()) {
            resobj = fc::mutable_variant_object
                ("primary_key", string(obj["block_num"].as<block_id_type>()))
                ("block_id_askey", obj["id"])
                ("previous", obj["header"].get_object()["previous"])
                ("block_num_askey",obj["block_num"])
                ("block_time", obj["header"].get_object()["timestamp"])
                ("producer", obj["header"].get_object()["producer"])
            ;
            vector <transaction_id_type> trxids;
            for (auto trxobj : obj["block"].get_object()["transactions"].get_array()) {
                auto trx = trxobj.as<transaction_receipt>();
                if (trx.trx.contains<transaction_id_type>())
                    trxids.push_back(trx.trx.get<transaction_id_type>());
                else
                    trxids.push_back(trx.trx.get<packed_transaction>().id());
            }
            resobj.set("trxs_num", static_cast<uint32_t>(trxids.size()));
        } else {
            resobj = fc::mutable_variant_object
                ("primary_key", string(obj["block_num"].as<block_id_type>()))
                ("irreversible", obj["irreversible"].as<bool>())
            ;
        }
        string block_time = obj["header"].get_object()["timestamp"].as<string>();
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        if (pos != string::npos && pos > 2) {
            if (block_time.length() > pos - 2) {
                block_time = block_time.substr(0, pos - 2);
                resobj.set("table_suffix", block_time);
            }
        }
        string key = string(obj["id"].as<block_id_type>()); 
        key += obj["irreversible"].as<bool>() ? 'T' : 'F';
        res.push_back({key, resobj});
        return res;
    }
};
static auto _block_info = types().register_type<BlockInfo>();

struct Transaction : type<Transaction> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res; 
        auto resobj = fc::mutable_variant_object
            ("block_num_askey", obj["block_num"])
            ("block_time", obj["block_time"])
            ("primary_key", ttp->id)
        ;
        if (ttp->receipt) {
            auto receipt_header = *(ttp->receipt);
            resobj.set("cpu_usage_us", static_cast<uint32_t>(receipt_header.cpu_usage_us));
            resobj.set("net_usage_words", static_cast<uint32_t>(receipt_header.net_usage_words));
        }
        if (ttp->action_traces.size()) {
            resobj.set("first_action", string(ttp->action_traces[0].act.account));
            if (ttp->action_traces[0].act.authorization.size()) {
                resobj.set("first_actor" , string(ttp->action_traces[0].act.authorization[0].actor));
            }
        }
        string block_time = obj["block_time"].as<string>();
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        if (pos != string::npos && pos > 2) {
            if (block_time.length() > pos - 2) {
                block_time = block_time.substr(0, pos - 2);
                resobj.set("table_suffix", block_time);
            }
        }
        string key = resobj["primary_key"].as<string>();
        key += "FB";
        res.push_back({key, resobj});
        return res;
    }
};
static auto _transaction = types().register_type<Transaction>();

struct Action : type<Action> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res;
        vector<fc::variant> action_traces_vector;
        if (obj.find("action_traces") != obj.end()) {
            queue<fc::variant> todo_action_traces;
            for (auto trace : obj["action_traces"].get_array()) {
                fc::mutable_variant_object traceobj(trace);
                traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                traceobj.set("parent_global_sequence", -1);
                action_traces_vector.push_back(traceobj);
                if (!trace.get_object()["inline_traces"].get_array().empty())
                    todo_action_traces.push(trace);
            }
            while (!todo_action_traces.empty()) {
                auto trace = todo_action_traces.front();
                todo_action_traces.pop();
                for (auto itrace : trace.get_object()["inline_traces"].get_array()) {
                    fc::mutable_variant_object traceobj(itrace);
                    traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                    traceobj.set("parent_global_sequence", trace.get_object()["receipt"].get_object()["global_sequence"]);
                    action_traces_vector.push_back(traceobj);
                    if (!itrace.get_object()["inline_traces"].get_array().empty())
                        todo_action_traces.push(itrace);
                }
            }
        }
        //基本信息
        auto block_time = obj["block_time"].as<string>();
        auto block_num  = ttp->block_num;
        auto trx_id = string(ttp->id);
        //计算分表策略
        fc::optional<string> table_suffix;
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        table_suffix = block_time.substr(0, pos - 2);

        for (auto var : action_traces_vector) {
            auto trace = var.get_object();
            auto resobj = fc::mutable_variant_object
                ("transaction_id", trace["trx_id"])
                ("block_time", ttp->block_time)
                ("block_num", ttp->block_num)
                ("table_suffix", table_suffix)
                ("account_askey", trace["act"].get_object()["account"])
                ("name_askey", trace["act"].get_object()["name"])
                ("receiver_askey", trace["receipt"].get_object()["receiver"])
                ("authorization", fc::json::to_string(trace["act"].get_object()["authorization"], fc::json::legacy_generator))
                ("data", fc::json::to_string(trace["act"].get_object()["data"]))
            ;
            if (trace["act"].get_object()["authorization"].is_array()) {
                auto authorization = trace["act"].get_object()["authorization"].get_array();
                if (!authorization.empty()) {
                    resobj.set("first_actor", authorization[0].get_object()["actor"]);
                }
            }
            string key = build_action_key(trace);
            resobj.set("primary_key", key);
            res.push_back({key, resobj});
        }
        return res;
    }
};
static auto _action = types().register_type<Action>();

struct BosBank : type<BosBank> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        //从transaction中获取所有的action_trace
        vector<fc::variant> action_traces_vector;
        if (obj.find("action_traces") != obj.end()) {
            queue<fc::variant> todo_action_traces;
            for (auto trace : obj["action_traces"].get_array()) {
                fc::mutable_variant_object traceobj(trace);
                traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                traceobj.set("parent_global_sequence", -1);
                action_traces_vector.push_back(traceobj);
                if (!trace.get_object()["inline_traces"].get_array().empty())
                    todo_action_traces.push(trace);
            }
            while (!todo_action_traces.empty()) {
                auto trace = todo_action_traces.front();
                todo_action_traces.pop();
                for (auto itrace : trace.get_object()["inline_traces"].get_array()) {
                    fc::mutable_variant_object traceobj(itrace);
                    traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                    traceobj.set("parent_global_sequence", trace.get_object()["receipt"].get_object()["global_sequence"]);
                    action_traces_vector.push_back(traceobj);
                    if (!itrace.get_object()["inline_traces"].get_array().empty())
                        todo_action_traces.push(itrace);
                }
            }
        }
        //基本信息
        auto block_time = obj["block_time"].as<string>();
        auto block_num  = ttp->block_num;
        auto trx_id = string(ttp->id);
        //计算分表策略
        fc::optional<string> table_suffix;
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        table_suffix = block_time.substr(0, pos - 2);
        //遍历action_trace筛选符合条件的action_trace
        key_values res;
        vector<string> accounts = {"btc.bos", "eth.bos", "usdt.bos"};
        vector<string> names = {"deposit", "withdraw", "transfer"};
        for (auto var : action_traces_vector) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<account_name>());
            string receiver = string(trace["receipt"].get_object()["receiver"].as<account_name>());
            if (receiver == account && contains(accounts, account) && contains(names, name)) {
                auto data = trace["act"].get_object()["data"].get_object();
                fc::optional<string> from, to, memo;
                fc::optional<asset>  quantity;
                fc::optional<string> inoutrecords; //兼容处理,由于deposit的to和withdraw的from表示同样的意义,这里把他们统一放在一个列里
                if (data.find("from") != data.end()) {
                    from = data["from"].as<string>();
                    if (name == "withdraw") {
                        inoutrecords = data["from"].as<string>();
                    }
                }
                if (data.find("to") != data.end()) {
                    to = data["to"].as<string>();
                    if (name == "deposit") {
                        inoutrecords = data["to"].as<string>();
                    }
                }
                if (data.find("memo") != data.end()) {
                    memo = data["memo"].as<string>();
                }
                if (data.find("quantity") != data.end()) {
                    quantity = data["quantity"].as<asset>();
                }
                auto resobj= fc::mutable_variant_object
                    ("transaction_id", trx_id)
                    ("block_time", obj["block_time"])
                    ("block_num",  block_num)
                    ("table_suffix", table_suffix)
                    ("account", account)
                    ("name", name)
                    ("data", fc::json::to_string(data, fc::json::legacy_generator))
                ;
                if (from) {
                    resobj.set("from", from);
                }
                if (to) {
                    resobj.set("to", to);
                }
                if (quantity) {
                    resobj.set("amount", quantity->to_real());
                    resobj.set("symbol", quantity->symbol_name());
                }
                if (memo) {
                    resobj.set("memo", memo);
                }
                if (inoutrecords) {
                    resobj.set("inoutrecords", inoutrecords);
                }
                string key = build_action_key(trace);
                resobj.set("primary_key", key);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _bosbank = types().register_type<BosBank>();

struct Uid : type<Uid> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        //从transaction中获取所有的action_trace
        vector<fc::variant> action_traces_vector;
        if (obj.find("action_traces") != obj.end()) {
            queue<fc::variant> todo_action_traces;
            for (auto trace : obj["action_traces"].get_array()) {
                fc::mutable_variant_object traceobj(trace);
                traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                traceobj.set("parent_global_sequence", -1);
                action_traces_vector.push_back(traceobj);
                if (!trace.get_object()["inline_traces"].get_array().empty())
                    todo_action_traces.push(trace);
            }
            while (!todo_action_traces.empty()) {
                auto trace = todo_action_traces.front();
                todo_action_traces.pop();
                for (auto itrace : trace.get_object()["inline_traces"].get_array()) {
                    fc::mutable_variant_object traceobj(itrace);
                    traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                    traceobj.set("parent_global_sequence", trace.get_object()["receipt"].get_object()["global_sequence"]);
                    action_traces_vector.push_back(traceobj);
                    if (!itrace.get_object()["inline_traces"].get_array().empty())
                        todo_action_traces.push(itrace);
                }
            }
        }
        //基本信息
        auto block_time = obj["block_time"].as<string>();
        auto block_num  = ttp->block_num;
        auto trx_id = string(ttp->id);
        //计算分表策略
        fc::optional<string> table_suffix;
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        table_suffix = block_time.substr(0, pos - 2);
        //遍历action_trace筛选符合条件的action_trace
        key_values res;
        for (auto var : action_traces_vector) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<account_name>());
            string receiver = string(trace["receipt"].get_object()["receiver"].as<account_name>());
            if (receiver == account && account == "uid" && name == "charge") {
                auto data = trace["act"].get_object()["data"].get_object();
                fc::optional<string> username, contract, memo;
                fc::optional<asset> quantity;
                if (data.find("username") != data.end()) {
                    username = data["username"].as<string>();
                }
                if (data.find("contract") != data.end()) {
                    contract = data["contract"].as<string>();
                }
                if (data.find("memo") != data.end()) {
                    memo = data["memo"].as<string>();
                }
                if (data.find("quantity") != data.end()) {
                    quantity = data["quantity"].as<asset>();
                }
                auto resobj= fc::mutable_variant_object
                    ("transaction_id", trx_id)
                    ("block_time", obj["block_time"])
                    ("block_num",  block_num)
                    ("table_suffix", table_suffix)
                    ("account", account)
                    ("name", name)
                    ("data", fc::json::to_string(data, fc::json::legacy_generator))
                ;
                if (username) {
                    resobj.set("from", username);
                }
                if (contract) {
                    resobj.set("to", contract);
                }
                if (quantity) {
                    resobj.set("amount", quantity->to_real());
                    resobj.set("symbol", quantity->symbol_name());
                }
                if (memo) {
                    resobj.set("memo", memo);
                }
                string key = build_action_key(trace);
                resobj.set("primary_key", key);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _uid = types().register_type<Uid>();

struct Transfer : type<Transfer> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        //从transaction中获取所有的action_trace
        vector<fc::variant> action_traces_vector;
        if (obj.find("action_traces") != obj.end()) {
            queue<fc::variant> todo_action_traces;
            for (auto trace : obj["action_traces"].get_array()) {
                fc::mutable_variant_object traceobj(trace);
                traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                traceobj.set("parent_global_sequence", -1);
                action_traces_vector.push_back(traceobj);
                if (!trace.get_object()["inline_traces"].get_array().empty())
                    todo_action_traces.push(trace);
            }
            while (!todo_action_traces.empty()) {
                auto trace = todo_action_traces.front();
                todo_action_traces.pop();
                for (auto itrace : trace.get_object()["inline_traces"].get_array()) {
                    fc::mutable_variant_object traceobj(itrace);
                    traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                    traceobj.set("parent_global_sequence", trace.get_object()["receipt"].get_object()["global_sequence"]);
                    action_traces_vector.push_back(traceobj);
                    if (!itrace.get_object()["inline_traces"].get_array().empty())
                        todo_action_traces.push(itrace);
                }
            }
        }
        //基本信息
        auto block_time = obj["block_time"].as<string>();
        auto block_num  = ttp->block_num;
        auto trx_id = string(ttp->id);
        //计算分表策略
        fc::optional<string> table_suffix;
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        table_suffix = block_time.substr(0, pos - 2);
        //遍历action_trace筛选符合条件的action_trace
        key_values res;
        for (auto var : action_traces_vector) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<account_name>());
            string receiver = string(trace["receipt"].get_object()["receiver"].as<account_name>());
            if (receiver == account && account == "eosio.token" && name == "transfer") {
                auto data = trace["act"].get_object()["data"].get_object();
                fc::optional<string> from, to, memo;
                fc::optional<asset> quantity;
                if (data.find("from") != data.end()) {
                    from = data["from"].as<string>();
                }
                if (data.find("to") != data.end()) {
                    to = data["to"].as<string>();
                }
                if (data.find("memo") != data.end()) {
                    memo = data["memo"].as<string>();
                }
                if (data.find("quantity") != data.end()) {
                    quantity = data["quantity"].as<asset>();
                }
                if (!from || !to || !quantity) {
                    continue;
                }
                auto resobj= fc::mutable_variant_object
                    ("transaction_id", trx_id)
                    ("block_time", obj["block_time"])
                    ("block_num",  block_num)
                    ("table_suffix", table_suffix)
                    ("account", account)
                    ("name", name)
                    ("data", fc::json::to_string(data, fc::json::legacy_generator))
                ;
                if (from) {
                    resobj.set("from", from);
                }
                if (to) {
                    resobj.set("to", to);
                }
                if (quantity) {
                    resobj.set("amount", quantity->to_real());
                    resobj.set("symbol", quantity->symbol_name());
                }
                if (memo) {
                    resobj.set("memo", memo);
                }
                string key = build_action_key(trace);
                resobj.set("primary_key", key);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _transfer = types().register_type<Transfer>();

struct Ibc : type<Ibc> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        //从transaction中获取所有的action_trace
        vector<fc::variant> action_traces_vector;
        if (obj.find("action_traces") != obj.end()) {
            queue<fc::variant> todo_action_traces;
            for (auto trace : obj["action_traces"].get_array()) {
                fc::mutable_variant_object traceobj(trace);
                traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                traceobj.set("parent_global_sequence", -1);
                action_traces_vector.push_back(traceobj);
                if (!trace.get_object()["inline_traces"].get_array().empty())
                    todo_action_traces.push(trace);
            }
            while (!todo_action_traces.empty()) {
                auto trace = todo_action_traces.front();
                todo_action_traces.pop();
                for (auto itrace : trace.get_object()["inline_traces"].get_array()) {
                    fc::mutable_variant_object traceobj(itrace);
                    traceobj.set("index_in_transaction", static_cast<uint32_t>(action_traces_vector.size()));
                    traceobj.set("parent_global_sequence", trace.get_object()["receipt"].get_object()["global_sequence"]);
                    action_traces_vector.push_back(traceobj);
                    if (!itrace.get_object()["inline_traces"].get_array().empty())
                        todo_action_traces.push(itrace);
                }
            }
        }
        //基本信息
        auto block_time = obj["block_time"].as<string>();
        auto block_num  = ttp->block_num;
        auto trx_id = string(ttp->id);
        //计算分表策略
        fc::optional<string> table_suffix;
        size_t replace_pos = block_time.find("-");
        while (replace_pos != string::npos) {
            block_time.replace(replace_pos, 1, "");
            replace_pos = block_time.find("-");
        }
        size_t pos = block_time.find('T');
        table_suffix = block_time.substr(0, pos - 2);
        //遍历action_trace筛选符合条件的action_trace
        key_values res;
        vector<string> accounts = {"bosibc.io", "eosio.token"};
        vector<string> names = {"transfer"};
        for (auto var : action_traces_vector) {
            auto trace = var.get_object();
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<account_name>());
            string receiver = string(trace["receipt"].get_object()["receiver"].as<account_name>());
            if (receiver == account && contains(accounts, account) && contains(names, name)) {
                auto data = trace["act"].get_object()["data"].get_object();
                fc::optional<string> from, to, memo;
                fc::optional<asset>  quantity;
                if (data.find("from") != data.end()) {
                    from = data["from"].as<string>();
                }
                if (data.find("to") != data.end()) {
                    to = data["to"].as<string>();
                }
                if (data.find("memo") != data.end()) {
                    memo = data["memo"].as<string>();
                }
                if (data.find("quantity") != data.end()) {
                    quantity = data["quantity"].as<asset>();
                }
                auto resobj= fc::mutable_variant_object
                    ("transaction_id", trx_id)
                    ("block_time", obj["block_time"])
                    ("block_num",  block_num)
                    ("table_suffix", table_suffix)
                    ("account", account)
                    ("name", name)
                    ("data", fc::json::to_string(data, fc::json::legacy_generator))
                ;
                if (from) {
                    resobj.set("from", from);
                }
                if (to) {
                    resobj.set("to", to);
                }
                if (*from != "bosibc.io" && *to != "bosibc.io") {
                    continue;
                }
                if (quantity) {
                    resobj.set("amount", quantity->to_real());
                    resobj.set("symbol", quantity->symbol_name());
                }
                if (memo) {
                    resobj.set("memo", memo);
                }
                string key = build_action_key(trace);
                resobj.set("primary_key", key);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _ibc = types().register_type<Ibc>();
 
}}} //eosio::data::es
