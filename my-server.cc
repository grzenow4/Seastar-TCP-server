#include <iostream>
#include <map>
#include <regex>
#include <vector>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>

static std::string done = "DONE$";
static std::string found = "FOUND$";
static std::string not_found = "NOTFOUND$";
static std::string input_err = "INPUTERROR";
static std::regex store_reg("STORE\\$[a-z]*\\$[a-z]*\\$");
static std::regex load_reg("LOAD\\$[a-z]*\\$");

using namespace seastar;

class tcp_server {
    std::vector<server_socket> _tcp_listeners;
    std::map<std::string, std::string> _data;
public:
    future<> listen(ipv4_addr addr) {
        listen_options lo;
        lo.proto = transport::TCP;
        lo.reuse_address = true;
        _tcp_listeners.push_back(seastar::listen(make_ipv4_address(addr), lo));
        do_accepts(_tcp_listeners);
        return make_ready_future<>();
    }

    future<> stop() {
        return make_ready_future<>();
    }

    void do_accepts(std::vector<server_socket>& listeners) {
        int which = listeners.size() - 1;
        (void)listeners[which].accept().then([this, &listeners] (accept_result ar) mutable {
            connected_socket fd = std::move(ar.connection);
            socket_address addr = std::move(ar.remote_address);
            auto conn = new connection(*this, std::move(fd), addr);
            (void)conn->process().then_wrapped([conn] (auto&& f) {
                delete conn;
                try {
                    f.get();
                } catch (std::exception& ex) {
                    std::cout << "request error " << ex.what() << "\n";
                }
            });
            do_accepts(listeners);
        }).then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (std::exception& ex) {
                std::cout << "accept failed: " << ex.what() << "\n";
            }
        });
    }

    future<> store(std::string key, std::string value) {
        _data[key] = value;
        return make_ready_future<>();
    }

    future<std::optional<std::string>> load(std::string key) {
        return _data.find(key) != _data.end()
            ? make_ready_future<std::optional<std::string>>(_data[key])
            : make_ready_future<std::optional<std::string>>(std::nullopt);
    }

    class connection {
        tcp_server& _server;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
    public:
        connection(tcp_server& server, connected_socket&& fd, socket_address addr)
            : _server(server)
            , _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output()) {}

        future<> process() {
            return read().then([this] (std::string cmd) {
                std::cout << "Dostałem " << cmd << '\n';
                return write(input_err);
            });
        }

        future<std::string> read_once() {
            if (_read_buf.eof()) {
                return make_ready_future<std::string>();
            }

            return do_with(std::string(), [this] (auto& buffer) {
                return do_until([&buffer] { return !buffer.empty() && buffer.back() == '$'; }, [this, &buffer] {
                    return _read_buf.read_exactly(1)
                                    .then([] (temporary_buffer<char> tmp) { return tmp[0]; })
                                    .then([&buffer] (char c) { buffer.push_back(c); });
                }).then([this, &buffer] {
                    return buffer;
                });
            });
        }

        future<std::string> read() {
            std::string cmd = co_await read_once();
            if (cmd == "STORE$") {
                cmd += co_await read_once();
                cmd += co_await read_once();
            } else if (cmd == "LOAD$") {
                cmd += co_await read_once();
            }
            co_return make_ready_future<std::string>(cmd);
        }

        future<> write(std::string msg) {
            return _write_buf.write(msg).then([this] {
                return _write_buf.flush();
            });
        }

        future<> do_store(std::string key, std::string value) {
            return _server.store(key, value).then([this] {
                return write(done);
            }).then([this] {
                return this->process();
            });
        }

        future<> do_load(std::string key) {
            return _server.load(key).then([this] (auto res) {
                std::string msg;
                if (res.has_value()) {
                    msg = found + res.value() + '$';
                } else {
                    msg = not_found;
                }
                return write(msg);
            }).then([this] {
                return this->process();
            });
        }
    };
};

int main(int ac, char** av) {
    app_template app;
    return app.run_deprecated(ac, av, [&] {
        uint16_t port = 5555;
        auto server = new distributed<tcp_server>;
        (void)server->start().then([server = std::move(server), port] () mutable {
            engine().at_exit([server] {
                return server->stop();
            });
            (void)server->invoke_on_all(&tcp_server::listen, ipv4_addr{port});
        }).then([port] {
            std::cout << "Seastar TCP server listening on port " << port << " ...\n";
        });
    });
}
