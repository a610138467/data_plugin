#include <algorithm>
#include <eosio/data_plugin/data_plugin.hpp>
#include <eosio/data_plugin/producers.hpp>
#include <eosio/data_plugin/types.hpp>

namespace eosio {

static auto _data_plugin = app().register_plugin<data_plugin>();

void data_plugin::set_program_options(options_description& cli, options_description& cfg) {
    for (auto producer : eosio::data::producers().get_all_producers()) {
        producer.second->set_program_options(cli, cfg);
    }
    cfg.add_options()
        ("data-plugin-start-num", bpo::value<uint32_t>()->default_value(0),   "when will start")
        ("data-plugin-stop-num",  bpo::value<uint32_t>()->default_value(-1),  "when will stop, for test")
        ("data-plugin-struct",    bpo::value<vector<string> >()->composing(), "which struct will be record, can have more than one")
        ("data-plugin-producer",  bpo::value<vector<string> >()->composing(), "which producer will be used, can have more than one")
        ("data-plugin-prefix",    bpo::value<string>()->default_value("eosio"),"the prefix of all data struct name")
        ("data-plugin-register-accepted-block", bpo::value<bool>()->default_value(true), "if register callback on accepted block")
        ("data-plugin-register-irreversible-block", bpo::value<bool>()->default_value(true), "if register callback on irreversible block")
        ("data-plugin-register-applied-transaction", bpo::value<bool>()->default_value(true), "if register callback on applied transaction")
        ("data-plugin-register-accepted-transaction", bpo::value<bool>()->default_value(true), "if register callback on accepted transaction")
        ;
}

template <class T>
void callback(const vector<string>& types, const vector<string>& producers, const T& t, fc::optional<bool> irreversible = fc::optional<bool>()) {
    auto tvariant = app().get_plugin<chain_plugin>()
                        .chain().to_variant_with_abi(t, fc::seconds(10));
    fc::mutable_variant_object tobject = tvariant.get_object();
    if (irreversible)
        tobject.set("irreversible", *irreversible);
    for (string tname : types) {
        auto type = eosio::data::types().find_type(tname);
        if (!type) continue;
        auto datum = type->build(t, tobject);
        for (auto data : datum) {
            for (auto pname : producers) {
                auto producer = eosio::data::producers().find_producer(pname);
                if (!producer) continue;
                producer->produce(type->name, data.first, data.second);
            }
        }
    }
};
void data_plugin::plugin_initialize(const variables_map& options) {
    ilog("Initialize data plugin");
    for (auto producer : eosio::data::producers().get_all_producers()) {
        producer.second->initialize(options);
    }
    start_block_num = options.at("data-plugin-start-num").as<uint32_t>();
    stop_block_num = options.at("data-plugin-stop-num").as<uint32_t>();
    types = options.at("data-plugin-struct").as<vector<string> >();
    for (auto type : types) {
        if (!eosio::data::types().find_type(type)) 
            wlog ("data-plugin initialize warnning : type ${type} not found", ("type", type));
    }
    producers = options.at("data-plugin-producer").as<vector<string> >();
    for (auto producer : producers) {
        if (!eosio::data::producers().find_producer(producer))
            wlog ("data-plugin initialize warnning : producer ${producer not found",
                    ("producer", producer));
    }
    current_block_num = 0;
    
    string prefix = options.at("data-plugin-prefix").as<string>();
    for (auto type : eosio::data::types().get_all_types()) {
        string dest_name = type.first;
        constexpr char replace_from[] = "::";
        constexpr char replace_to[] = ".";
        string::size_type pos = dest_name.find(replace_from);
        while (pos != string::npos) {
            dest_name = dest_name.replace(pos, sizeof(replace_from) - 1, replace_to);
            pos = dest_name.find(replace_from);
        }
        vector<string> ignore_names = {"eosio.", "data."};
        for (auto name : ignore_names) {
            pos = dest_name.find(name);
            if (pos != string::npos)
                dest_name.replace(pos, name.length(), "");
        }
        std::transform (dest_name.begin(), dest_name.end(), dest_name.begin(), ::tolower);
        type.second->name = prefix + "." + dest_name;
    }

    auto& chain = app().get_plugin<chain_plugin>().chain();
    if (options.at("data-plugin-register-accepted-block").as<bool>()) {
        on_accepted_block_connection = chain.accepted_block.connect([=](const block_state_ptr& block_state) {
            if (current_block_num < start_block_num) return;
            try{
                callback(types, producers, block_state, false);
            } catch (const std::exception& ex) {
                elog ("std Exception in data_plugin when accept block : ${ex}", ("ex", ex.what()));
            } catch ( fc::exception& ex) {
                wlog( "fc Exception in data_plugin when accept block : ${ex}", ("ex", ex.to_detail_string()) );
            } catch (...) {
                elog ("Unknown Exception in data_plugin when accept block");
            }
        });
    }
    if (options.at("data-plugin-register-irreversible-block").as<bool>()) {
        on_irreversible_block_connection = chain.irreversible_block.connect([=](const block_state_ptr& block_state) {
            current_block_num = block_state->block_num;
            if (current_block_num < start_block_num) return;
            try {
                callback(types, producers, block_state, true);
            } catch (const std::exception& ex) {
                elog ("std Exception in data_plugin when irreversible block : ${ex}", ("ex", ex.what()));
            } catch ( fc::exception& ex) {
                wlog( "fc Exception in data_plugin when irreversible block : ${ex}", ("ex", ex.to_detail_string()) );
            } catch (...) {
                elog ("Unknown Exception in data_plugin when irreversible block");
            }
            ilog ("data_plug, current irreversible block_id : ${current_block_num}", ("current_block_num", current_block_num));
            if (stop_block_num > start_block_num && current_block_num >= stop_block_num) {
                ilog ("data plugin stopped. [${from}-${to}]", ("from", start_block_num)("to", stop_block_num));
                plugin_shutdown();
                app().quit();
            }
        });
    }
    if (options.at("data-plugin-register-applied-transaction").as<bool>()) {
        on_applied_transaction_connection = chain.applied_transaction.connect([=](const transaction_trace_ptr& transaction_trace) {
            if (current_block_num < start_block_num) return;
            try {
                callback(types, producers, transaction_trace);
            } catch (const std::exception& ex) {
                elog ("std Exception in data_plugin when applied transaction : ${ex}", ("ex", ex.what()));
            } catch ( fc::exception& ex) {
                wlog( "fc Exception in data_plugin when applied transaction : ${ex}", ("ex", ex.to_detail_string()) );
            } catch (...) {
                elog ("Unknown Exception in data_plugin when applied transaction");
            }
       });
    }
    if (options.at("data-plugin-register-accepted-transaction").as<bool>()) {
        on_accepted_transaction_connection = chain.accepted_transaction.connect([=](const transaction_metadata_ptr& transaction_metadata) {
            if (current_block_num < start_block_num) return;
            try {
                callback(types, producers, transaction_metadata);
            } catch (const std::exception& ex) {
                elog ("std Exception in data_plugin when accepted transaction : ${ex}", ("ex", ex.what()));
            } catch ( fc::exception& ex) {
                wlog( "fc Exception in data_plugin when accepted transaction : ${ex}", ("ex", ex.to_detail_string()) );
            } catch (...) {
                elog ("Unknown Exception in data_plugin when accepted transaction");
            }
        });
    }
}

void data_plugin::plugin_startup() {
    ilog("Starting data_plugin");
    for (auto producer : eosio::data::producers().get_all_producers()) {
        producer.second->startup();
    }
}

void data_plugin::plugin_shutdown() {
    ilog("Stopping data_plugin");
    on_accepted_block_connection.disconnect();
    on_irreversible_block_connection.disconnect();
    on_applied_transaction_connection.disconnect();
    on_accepted_transaction_connection.disconnect();
    for (auto producer : eosio::data::producers().get_all_producers()) {
        producer.second->stop();
    }
}

}
