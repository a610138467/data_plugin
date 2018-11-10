#include <string>
#include <boost/asio.hpp>
#include <fc/io/json.hpp>
#include <fc/log/logger.hpp>
#include <fc/network/url.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/core.hpp>
#include <appbase/application.hpp>
#include <fc/exception/exception.hpp>
#include <eosio/data_plugin/producers.hpp>

namespace eosio{ namespace data{

using std::vector;
using std::thread;
using appbase::app;
using std::shared_ptr;
using boost::asio::ip::tcp;
using boost::asio::io_service;
using boost::asio::deadline_timer;
namespace http = boost::beast::http;
typedef shared_ptr<tcp::endpoint> endpoint_ptr;
typedef shared_ptr<http::request<http::string_body> > request_ptr;

struct HttpProducer : producer<HttpProducer> {

    void set_program_options(options_description& cli, options_description& cfg) {
        cfg.add_options()
            ("data-plugin-http-producer-addr", bpo::value<vector<string> >()->composing(), "the addr of http to callback, can have more than one")
            ("data-plugin-http-producer-try-num", bpo::value<uint32_t>()->default_value(5), "the maxmium time to retry if failed")
            ("data-plugin-http-producer-retry-interval", bpo::value<uint32_t>()->default_value(1000), "the interval ms between each retry")
            ("data-plugin-http-producer-max-wait", bpo::value<uint32_t>()->default_value(1000), "the max wait time for a request")
        ;
    }
    void initialize(const variables_map& options) {
        if (options.count("data-plugin-http-producer-addr") <= 0) return;
        vector<string> addrs = options["data-plugin-http-producer-addr"].as<vector<string> >(); 
        for (auto addr : addrs ) {
            urls.emplace_back(addr); 
        }
        if (addrs.empty()) return;
        initialized = true;

        try_num = options["data-plugin-http-producer-try-num"].as<uint32_t>();
        retry_interval = options["data-plugin-http-producer-retry-interval"].as<uint32_t>();
        max_wait = options["data-plugin-http-producer-max-wait"].as<uint32_t>();

        io_worker = std::make_shared<io_service::work>(io);
        io_thread = std::make_shared<thread>([&](){io.run();});
    }
    void startup() {
    }
    void produce (const string& name, const string& key, fc::variant& value) {
        if (!initialized) return;
        //step1 : crete data
        auto data = fc::mutable_variant_object
            ("table", name)
            ("data" , value)
        ;
        auto payload = fc::json::to_string(data, fc::json::legacy_generator);
        for (auto url : urls) {
            string path = url.path() ? url.path()->generic_string() : "/";
            if (url.query()) path += "?" + *url.query();
            request_ptr request = std::make_shared<http::request<http::string_body>>(http::verb::post, path, 11);
            request->set(http::field::host, *url.host());
            request->set(http::field::user_agent, "data-plugin");
            request->set(http::field::content_type, "application/json");
            request->keep_alive(true);
            request->body() = payload;
            request->prepare_payload();
            io.post([=](){
                async_send(key, url, request, 0);
            });
        }
    }

    void async_send(const std::string key, fc::url url, const request_ptr request, int loop) {
        if (loop > try_num) {
            elog ("in http-producer : request failed. [key=${key}] [url=${url}] [data=${data}]",
                    ("key", key)("url", string(url))("data", request->body()));
            return;
        } else if (loop > 0) {
            elog ("in http-producer : request error. try again(${loop}/${try_num}). [key=${key}] [url=${url}]",
                    ("key", key)("url", string(url))("loop", loop)("try_num", try_num));
        }
        boost::system::error_code errorcode;
        tcp::resolver resolver(io);
        tcp::resolver::query query(*url.host(), url.port() ? std::to_string(*url.port()) : "80");
        auto resolver_it = resolver.resolve(query, errorcode);
        if (errorcode) {
            elog ("data-plugin : http-producer : resolve addr ${host}:${port} failed . reason : ${reason}",
                    ("addr", *url.host())("port", *url.port())("reason", errorcode.message()));
        }
        auto endpoint = std::make_shared<tcp::endpoint>(resolver_it->endpoint());

        shared_ptr<deadline_timer> deadline = std::make_shared<deadline_timer>(
            io, boost::posix_time::milliseconds(max_wait));
        shared_ptr<tcp::socket> socket = std::make_shared<tcp::socket>(io);
        shared_ptr<bool> expired = std::make_shared<bool>(false);
        shared_ptr<bool> canceled= std::make_shared<bool>(false);
        //step1 : set timer
        deadline->async_wait([=](const boost::system::error_code& error) {
            if (*canceled) {
                dlog("timeout called , but already canceled");
                return;
            }
            if (error) {
                elog ("deadline timer error : ${ex}", ("ex", error.message()));
            }
            elog ("in http-producer : request timeout [key=${key}] [url=${url}]",
                    ("key", key)("url", string(url)));
            *expired = true;
            if (socket->is_open()) {
                dlog("socket is open cancel it");
                socket->cancel();
            }
            deadline->cancel();
            shared_ptr<deadline_timer> retry = std::make_shared<deadline_timer>(
                io, boost::posix_time::milliseconds(retry_interval));
            retry->async_wait([=](const boost::system::error_code& error) {
                if (error) {
                    elog ("retry timer error : ${ex}", ("ex", error.message()));
                }
                retry->cancel();
                async_send(key, url, request, loop + 1);
            });
        });
        //step2 : connect
        socket->async_connect(*endpoint, [=](const boost::system::error_code& error) {
            if (*expired) {
                dlog ("connect called , but already expired");
                return;
            }
            if (error) {
                elog ("in http-producer : request connect failed [key=${key}] [url=${url}] [err=${err}]",
                        ("key", key)("url", string(url))("err", error.message()));
                 *canceled = true;
                if (socket->is_open()) {
                    dlog("socket is open cancel it");
                    socket->cancel();
                }
                deadline->cancel();
                auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                retry->async_wait([=](const boost::system::error_code& error) {
                    if (error) {
                        elog ("retry timer error : ${ex}", ("ex", error.message()));
                    }
                    retry->cancel();
                    async_send(key, url, request, loop + 1);
                });
            }
            dlog ("connect finish [key=${key}] [url=${url}]", ("key", key)("url", string(url)));
            //step3 : send
            http::async_write(*socket, *request, [=](const boost::system::error_code& error, std::size_t len) {
                if (*expired) {
                    dlog ("write called , but already expired");
                    return;
                }
                if (error) {
                    elog ("in http-producer : request write failed [key=${key}] [url=${url} [err=${err}]",
                            ("key", key)("url", string(url))("err", error.message()));
                    *canceled = true;
                    if (socket->is_open()) {
                        socket->cancel();
                        dlog ("socket is open cancel it");
                        socket->cancel();
                    }
                    deadline->cancel();
                    auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                    retry->async_wait([=](const boost::system::error_code& error) {
                        if (error) {
                            elog ("retry timer error : ${ex}", ("ex", error.message()));
                        }
                        retry->cancel();
                        async_send(key, url, request, loop + 1);
                    });
                }
                dlog ("write finish [key=${key}] [url=${url}]", ("key", key)("url", string(url)));
                //step4 : read
                auto buffer = std::make_shared<boost::beast::flat_buffer>();
                auto response = std::make_shared<http::response<http::string_body>>();
                http::async_read(*socket, *buffer, *response, [=](const boost::system::error_code& error, std::size_t len) {
                    (void)buffer;
                    (void)response;
                    if (*expired) {
                        dlog ("read called , but already expired");
                        return;
                    }
                    if (error) {
                        elog ("in http-producer : request read failed [key=${key}] [url=${url}] [err=${err}]",
                                ("key", key)("url", string(url))("err", error.message()));
                        *canceled = true;
                        if (socket->is_open()) {
                            socket->cancel();
                            dlog ("socket is open cancel it");
                            socket->cancel();
                        }
                        deadline->cancel();
                        auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                        retry->async_wait([=](const boost::system::error_code& error) {
                            if (error) {
                                elog ("retry timer error : ${ex}", ("ex", error.message()));
                            }
                            retry->cancel();
                            async_send(key, url, request, loop + 1);
                        });
                    }
                    dlog ("read finish [key=${key}] [url=${url}] [code=${code}]", 
                        ("key", key)("url", string(url))("code", static_cast<int>(response->result())));
                    //step5 : analyse
                    printf ("test123\n");
                    if (response->result() != http::status::ok) {
                        elog ("in http-producer : response code error [key=${key}] [url=${ur}] [code=${code}]",
                                ("key", key)("url", string(url))("code", static_cast<int>(response->result())));
                        *canceled = true;
                        if (socket->is_open()) {
                            dlog ("socket is open cancel it");
                            socket->cancel();
                        }
                        deadline->cancel();
                        auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                        retry->async_wait([=](const boost::system::error_code& error) {
                            if (error) {
                                elog ("retry timer error : ${ex}", ("ex", error.message()));
                            }
                            retry->cancel();
                            async_send(key, url, request, loop + 1);
                        });
                        return;
                    }
                    try {
                        auto result = fc::json::from_string(response->body());
                        if ( !(result.is_object() 
                            && result.get_object().find("status") != result.get_object().end()
                            && result.get_object()["status"].as<int>() == 0))
                        {
                            throw fc::parse_error_exception();
                        }
                    } catch (const fc::parse_error_exception& ex) {
                        elog ("in http-producer : response body error [key=${key}] [url=${url}] [body=${body}]",
                                ("key", key)("url", string(url))("body", response->body()));
                        *canceled = true;
                        if (socket->is_open()) {
                            dlog ("socket is open cancel it");
                            socket->cancel();
                        }
                        deadline->cancel();
                        auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                        retry->async_wait([=](const boost::system::error_code& error) {
                            if (error) {
                                elog ("retry timer error : ${ex}", ("ex", error.message()));
                            }
                            retry->cancel();
                            async_send(key, url, request, loop + 1);
                        });
                        return;
                    } catch (const fc::eof_exception& ex) {
                        elog ("in http-producer : response body error [key=${key}] [url=${url}] [error=${error}]",
                                ("key", key)("url", string(url))("error", ex.what()));
                        *canceled = true;
                        if (socket->is_open()) {
                            dlog ("socket is open cancel it");
                            socket->cancel();
                        }
                        deadline->cancel();
                        auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                        retry->async_wait([=](const boost::system::error_code& error) {
                            if (error) {
                                elog ("retry timer error : ${ex}", ("ex", error.message()));
                            }
                            retry->cancel();
                            async_send(key, url, request, loop + 1);
                        });
                        return;
                    } catch (const std::exception& ex) {
                        elog ("in http-producer : response body error [key=${key}] [url=${url}] [error=${error}]",
                                ("key", key)("url", string(url))("error", ex.what()));
                        *canceled = true;
                        if (socket->is_open()) {
                            dlog ("socket is open cancel it");
                            socket->cancel();
                        }
                        deadline->cancel();
                        auto retry = std::make_shared<deadline_timer>(io, boost::posix_time::milliseconds(retry_interval));
                        retry->async_wait([=](const boost::system::error_code& error) {
                            if (error) {
                                elog ("retry timer error : ${ex}", ("ex", error.message()));
                            }
                            retry->cancel();
                            async_send(key, url, request, loop + 1);
                        });
                        return;
                    }
                    dlog ("produce finish [key=${key} [url=${url}]", ("key", key)("url", string(url)));
                    *canceled = true;
                    deadline->cancel();
                });
            });
        });
    }
    void stop() {
        ilog ("data-plugin http-producer begin stop");
        io.post([=](){
            io_worker.reset();
        });
        io_thread->join();
        ilog ("data-plugin http-producer stop finish");
    }

    vector<fc::url> urls;
    uint32_t try_num;
    uint32_t retry_interval;
    uint32_t max_wait;
    bool initialized;
    io_service io;
    shared_ptr<io_service::work> io_worker;
    shared_ptr<thread> io_thread;
};
static auto _http_producer = eosio::data::producers().register_producer<HttpProducer>();

}}
