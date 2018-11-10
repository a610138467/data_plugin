#include <eosio/data_plugin/types.hpp>

namespace eosio{ namespace data{ 

static auto  _types = std::make_shared<type_collection>();
type_collection& types() { 
    return *_types; 
}

}}
