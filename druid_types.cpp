#include <queue>
#include <string>
#include <vector>
#include <chrono>
#include <fc/io/json.hpp>
#include <eosio/chain/asset.hpp>
#include <boost/lexical_cast.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/data_plugin/types.hpp>

namespace eosio{ namespace data{ namespace druid{

using std::queue;
using std::string;
using std::vector;
using namespace chain;
using key_values = abstract_type::key_values;

struct Transfer : type<Transfer> {
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
        for (auto var : action_traces_vector) {
            auto trace = var.get_object();
            auto resobj = fc::mutable_variant_object
                ("transaction_id", trace["trx_id"])
                ("block_num", ttp->block_num)
                ("block_time", ttp->block_time)
            ;
            string account = string(trace["act"].get_object()["account"].as<account_name>());
            string name = string(trace["act"].get_object()["name"].as<account_name>());
            if (account == "eosio.token" && name == "transfer") {
                auto data = trace["act"].get_object()["data"].get_object();
                resobj.set("from", data["from"]);
                resobj.set("to", data["to"]);
                resobj.set("memo", data["memo"]);
                auto quantity = data["quantity"].as<asset>();
                resobj.set("symbol", quantity.symbol_name());
                resobj.set("amount", quantity.to_real());
                string key = build_action_key(trace);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _transfer = types().register_type<Transfer>();

struct Resource : type<Transfer> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res; 
        std::vector<string> actors;
        string receiver = "";
        if (ttp->receipt) {
            auto receipt_header = *(ttp->receipt);
            for (auto trace : ttp->action_traces) {
                for (auto auth : trace.act.authorization) {
                    auto actor = auth.actor;
                    if (std::find(actors.begin(), actors.end(), string(actor)) == actors.end()) {
                        actors.push_back(string(actor));
                    }
                }
                if (receiver == "") {
                    receiver = string(trace.receipt.receiver);
                }
            }
            for (auto actor : actors) {
                auto resobj = fc::mutable_variant_object
                    ("block_time", ttp->block_time)
                    ("trx", ttp->id)
                    ("cpu_usage_us", static_cast<uint32_t>(receipt_header.cpu_usage_us))
                    ("net_usage_words", static_cast<uint32_t>(receipt_header.net_usage_words))
                    ("actor", actor)
                    ("receiver",receiver)
                ;
                string key = string(ttp->id) + "|" + string(actor);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _resource = types().register_type<Resource>();

struct Active : type<Active> {
    key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& obj) {
        key_values res; 
        std::vector<string> actor_receivers;
        if (ttp->receipt) {
            auto receipt_header = *(ttp->receipt);
            for (auto trace : ttp->action_traces) {
                auto receiver = trace.receipt.receiver;
                auto action = trace.act.account;
                auto name = trace.act.name;
                for (auto auth : trace.act.authorization) {
                    auto actor = auth.actor;
                    string actor_receiver = string(actor) + ":" + string(receiver) + ":" + string(action) + "_" + string(name);
                    if (std::find(actor_receivers.begin(), actor_receivers.end(), actor_receiver) == actor_receivers.end()) {
                        actor_receivers.push_back(actor_receiver);
                    }
                }
            }
            for (auto actor_receiver : actor_receivers) {
                size_t first_pos = actor_receiver.find(":");
                string actor = actor_receiver.substr(0, first_pos);
                size_t secon_pos = actor_receiver.find(":", first_pos+1);
                string receiver = actor_receiver.substr(first_pos+1, secon_pos-first_pos-1);
                string action_name = actor_receiver.substr(secon_pos+1);
                auto resobj = fc::mutable_variant_object
                    ("block_time", ttp->block_time)
                    ("trx", ttp->id)
                    ("actor", actor)
                    ("receiver",receiver)
                    ("method", action_name)
                ;
                string key = string(ttp->id) + "|" + string(actor_receiver);
                res.push_back({key, resobj});
            }
        }
        return res;
    }
};
static auto _active = types().register_type<Active>();
 
}}} //eosio::data::es
