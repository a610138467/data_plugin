#include <eosio/data_plugin/producers.hpp>

namespace eosio{ namespace data{

static auto  _producers = std::make_shared<producer_collection>();

producer_collection& producers() { 
    return *_producers; 
}

}}
