#include <time.h>
#include <fstream>
#include <fc/log/logger.hpp>
#include <boost/filesystem.hpp>
#include <appbase/application.hpp>
#include <eosio/data_plugin/producers.hpp>

namespace eosio {namespace data{

using std::ios;
using std::ofstream;
namespace bfs = boost::filesystem;

struct FileProducer : producer<FileProducer> {
    void set_program_options(options_description& cli, options_description& cfg) {
        cfg.add_options()
            ("data-plugin-file-producer-file-name", bpo::value<string>()->default_value("production"),   "the file where to save content")
        ;
    }
    void initialize(const variables_map& options) {
        file_path = options["data-plugin-file-producer-file-name"].as<string>(); 
        if (file_path.is_relative())
            file_path = appbase::app().data_dir() / file_path;
        if(!bfs::exists(file_path.parent_path()))
            bfs::create_directories(file_path.parent_path());
    }
    void startup() {
        time_t now = time(NULL);
        tm* t = localtime(&now);
        if (current_hour != t->tm_hour) {
            char now_str[16];
            sprintf(now_str, "%04d%02d%02d%02d", t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour);
            if (file.is_open())
                file.close();
            string file_name = file_path.string() + "." + now_str;
            file.open(file_name, ios::out | ios::app | ios::binary);
            if (!file.is_open()) {
                wlog ("data-plugin file producer : open file ${file} failed", ("file", file_name));
            }
            current_hour = t->tm_hour;
        }
    }
    void stop() {
        if (file.is_open())
            file.close();
    }
    void produce (const string& name, const string& key, fc::variant& value) {
        time_t now = time(NULL);tm *t = localtime(&now);
        if (current_hour != t->tm_hour) {
            char now_str[16];
            sprintf(now_str, "%04d%02d%02d%02d", t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour);
            if (file.is_open())
                file.close();
            string file_name = file_path.string() + "." + now_str;
            file.open(file_name, ios::out | ios::app | ios::binary);
            if (!file.is_open()) {
                wlog ("data-plugin file producer : open file ${file} failed", ("file", file_name));
            }
            current_hour = t->tm_hour;
        }
        if (file.is_open() && !file.bad() && !file.fail()) {
            file << name << "\t" << key << "\t" << fc::json::to_string(value, fc::json::legacy_generator) << std::endl; 
        } else {
            wlog ("data-plugin file producer : file not open");
        }
    }

    bfs::path file_path;
    ofstream file;
    uint8_t current_hour = 25;
};
static auto _file_producer = producers().register_producer<FileProducer>();

}}
