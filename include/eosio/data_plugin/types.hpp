#pragma once
#include <map>
#include <string>
#include <vector>
#include <eosio/chain/controller.hpp>

namespace eosio{ namespace data{

using std::string;
using std::vector;
using std::pair;
using std::map;
using std::shared_ptr;
using namespace chain;

struct abstract_type {
    typedef vector<pair<string, variant> > key_values;

    virtual key_values build(const block_state_ptr& bsp, const fc::mutable_variant_object& o) {
        return key_values();
    }
    virtual key_values build(const transaction_trace_ptr& ttp, const fc::mutable_variant_object& o) {
        return key_values();
    }
    virtual key_values build(const transaction_metadata_ptr& tmp, const fc::mutable_variant_object& o) {
        return key_values();
    }

    std::string name;
};

template <typename successor>
struct type : abstract_type {
    
};

struct type_collection {
    typedef map<string, shared_ptr<abstract_type> > type_map;
    type_map types;
    
    template <typename T>
    abstract_type* register_type () {
        abstract_type* t = new T();
        string type_name = boost::core::demangle(typeid(T).name());
        types[type_name].reset(t);
        return t;
    }
    abstract_type* find_type(const std::string& type_name) {
        if (types.find(type_name) == types.end())
            return NULL;
        else
            return types[type_name].get();
    }
    type_map& get_all_types() {
        return types;
    }
};

type_collection& types();

inline 
string build_action_key(const fc::variant_object& trace) {
    string key = string(trace["trx_id"].as<transaction_id_type>());
    auto index_in_transaction = trace["index_in_transaction"].as<uint32_t>();
    const char* index_ptr = (const char*)(&index_in_transaction);
    for (auto i = 0; i < sizeof(index_in_transaction); i ++) {
        char tmp[8];
        sprintf (tmp, "%02x", index_ptr[i]);
        key += tmp;
    }
    return key;
}


}}

FC_REFLECT(eosio::chain::transaction_metadata,
    (id)(signed_id)(packed_trx)(signing_keys)(accepted))
