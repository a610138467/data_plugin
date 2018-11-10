#pragma once
#include <queue>
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>

namespace eosio {

using std::string;
using std::vector;
using std::queue;
using namespace chain;
using namespace appbase;

class data_plugin : public appbase::plugin<data_plugin> {
public:
    APPBASE_PLUGIN_REQUIRES((chain_plugin))

    void set_program_options(options_description&, options_description& cfg) override;
    void plugin_initialize(const variables_map& options);
    void plugin_startup();
    void plugin_shutdown();

private:

    boost::signals2::connection on_accepted_block_connection;
    boost::signals2::connection on_irreversible_block_connection;
    boost::signals2::connection on_applied_transaction_connection;
    boost::signals2::connection on_accepted_transaction_connection;
    
    uint32_t start_block_num;
    uint32_t stop_block_num;
    vector<string> types;
    vector<string> producers;
    uint32_t current_block_num;
};

}
