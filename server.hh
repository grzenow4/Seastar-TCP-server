#pragma once

#include <iostream>
#include <map>
#include <regex>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

const std::string done = "DONE$";
const std::string found = "FOUND$";
const std::string not_found = "NOTFOUND$";
const std::regex store_reg("STORE\\$[a-z]*\\$[a-z]*\\$");
const std::regex load_reg("LOAD\\$[a-z]*\\$");

class tcp_server {
    std::vector<seastar::server_socket> _tcp_listeners;
    std::map<std::string, std::string> _data;
public:
    seastar::future<> listen(seastar::ipv4_addr addr);

    seastar::future<> stop();

    void do_accepts(std::vector<seastar::server_socket>& listeners);

    seastar::future<> store(std::string key, std::string value);

    seastar::future<std::optional<std::string>> load(std::string key);

    class connection {
        tcp_server& _server;
        seastar::connected_socket _fd;
        seastar::input_stream<char> _read_buf;
        seastar::output_stream<char> _write_buf;
    public:
        connection(tcp_server& server, seastar::connected_socket&& fd, seastar::socket_address addr);

        seastar::future<> process();

        seastar::future<std::string> read_once();

        seastar::future<std::string> read();

        seastar::future<> write(std::string msg);

        seastar::future<> do_store(std::string key, std::string value);

        seastar::future<> do_load(std::string key);
    };
};
