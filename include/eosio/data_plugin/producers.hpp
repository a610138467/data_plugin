#pragma once

#include <map>
#include <string>
#include <vector>
#include <fc/variant.hpp>
#include <fc/io/json.hpp>
#include <boost/program_options.hpp>

namespace eosio{ namespace data{

using std::string;
using std::map;
using std::shared_ptr;
using std::unique_ptr;
using fc::variant;
using boost::program_options::variables_map;
using boost::program_options::options_description;
namespace bpo = boost::program_options;

struct abstract_producer {
    virtual void produce (const string& name, const string& key, fc::variant& value) = 0;
    virtual void set_program_options(options_description& cli, options_description& cfg) = 0;
    virtual void initialize(const variables_map& options) = 0;
    virtual void startup() = 0;
    virtual void stop() = 0;
};

template <typename successor>
struct producer : abstract_producer {
    
};

struct producer_collection {
    typedef map<string, shared_ptr<abstract_producer>> producer_map;
    producer_map producers;

    template <typename P>
    abstract_producer* register_producer() {
        string producer_name = boost::core::demangle(typeid(P).name());
        abstract_producer* p = new P();
        producers[producer_name].reset(p);;
        return p;
    }
    abstract_producer* find_producer(const string& producer_name) {
        if (producers.find(producer_name) == producers.end())
            return NULL;
        else
            return producers[producer_name].get();
    } 
    producer_map& get_all_producers() {
        return producers;
    }
};

producer_collection& producers();

}}
