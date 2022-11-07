#include "server.hh"

using namespace seastar;

future<> tcp_server::listen(ipv4_addr addr) {
    listen_options lo;
    lo.proto = transport::TCP;
    lo.reuse_address = true;
    _tcp_listeners.push_back(seastar::listen(make_ipv4_address(addr), lo));
    do_accepts(_tcp_listeners);
    return make_ready_future<>();
}

future<> tcp_server::stop() {
    return make_ready_future<>();
}

void tcp_server::do_accepts(std::vector<server_socket>& listeners) {
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

future<> tcp_server::store(std::string key, std::string value) {
    _data[key] = value;
    return make_ready_future<>();
}

future<std::optional<std::string>> tcp_server::load(std::string key) {
    return _data.find(key) != _data.end()
        ? make_ready_future<std::optional<std::string>>(_data[key])
        : make_ready_future<std::optional<std::string>>(std::nullopt);
}

tcp_server::connection::connection(tcp_server& server, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output()) {}

future<> tcp_server::connection::process() {
    auto cmd = co_await read();

    if (regex_match(cmd, store_reg)) {
        std::stringstream ss(cmd);
        std::string command, key, value;
        getline(ss, command, '$');
        getline(ss, key, '$');
        getline(ss, value, '$');
        co_return co_await do_store(key, value);
    } else if (regex_match(cmd, load_reg)) {
        std::stringstream ss(cmd);
        std::string command, key;
        getline(ss, command, '$');
        getline(ss, key, '$');
        co_return co_await do_load(key);
    }
    co_return co_await write(input_err);
}

future<std::string> tcp_server::connection::read_once() {
    if (_read_buf.eof()) {
        co_return co_await make_ready_future<std::string>();
    }

    std::string buffer;
    while (buffer.empty() || buffer.back() != '$') {
        auto tmp = co_await _read_buf.read_exactly(1);
        buffer.push_back(tmp[0]);
    }

    co_return buffer;
}

future<std::string> tcp_server::connection::read() {
    std::string cmd = co_await read_once();
    if (cmd == "STORE$") {
        cmd += co_await read_once();
        cmd += co_await read_once();
    } else if (cmd == "LOAD$") {
        cmd += co_await read_once();
    }
    co_return co_await make_ready_future<std::string>(cmd);
}

future<> tcp_server::connection::write(std::string msg) {
    co_await _write_buf.write(msg);
    co_await _write_buf.flush();
}

future<> tcp_server::connection::do_store(std::string key, std::string value) {
    co_await _server.store(key, value);
    co_await write(done);
    co_return co_await this->process();
}

future<> tcp_server::connection::do_load(std::string key) {
    auto res = co_await _server.load(key);
    std::string msg;
    if (res.has_value()) {
        msg = found + res.value() + '$';
    } else {
        msg = not_found;
    }
    co_await write(msg);
    co_return co_await this->process();
}
