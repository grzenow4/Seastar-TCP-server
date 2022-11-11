#include "server.hh"

using namespace seastar;

static int calc_hash(const std::string& s) {
    int res = 0;
    for (int c: s) {
        res += c;
    }
    std::cout << "Hash dla słowa " << s << ": " << res % smp::count << '\n';
    return res % smp::count;
}

future<> tcp_server::listen(ipv4_addr addr) {
    listen_options lo;
    lo.proto = transport::TCP;
    lo.reuse_address = true;
    _tcp_listener = seastar::listen(make_ipv4_address(addr), lo);
    do_accept(_tcp_listener);
    return make_ready_future<>();
}

future<> tcp_server::stop() {
    return make_ready_future<>();
}

void tcp_server::do_accept(server_socket& listener) {
    (void)listener.accept().then([this, &listener] (accept_result ar) mutable {
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
        do_accept(listener);
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            std::cout << "accept failed: " << ex.what() << "\n";
        }
    });
}

future<> tcp_server::store(const std::string& key, const std::string& value) {
    std::cout << "store() wywołane dla klucza " << key << " na shardzie: " << this_shard_id << '\n';
    _data[key] = value;
    return make_ready_future<>();
}

future<std::optional<std::string>> tcp_server::load(const std::string& key) {
    std::cout << "load() wywołane dla klucza " << key << " na shardzie: " << this_shard_id << '\n';
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
        int which = calc_hash(key);
        co_return co_await _server.container().invoke_on(which, [this, key, value] (auto& tcp_server) {
            // działaj
            return this->do_store(key, value);
        });
    } else if (regex_match(cmd, load_reg)) {
        std::stringstream ss(cmd);
        std::string command, key;
        getline(ss, command, '$');
        getline(ss, key, '$');
        int which = calc_hash(key);
        co_return co_await _server.container().invoke_on(which, [this, key] (auto& tcp_server) {
            // działaj
            return this->do_load(key);
        });
    }
    co_return co_await make_ready_future<>();
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

future<> tcp_server::connection::write(const std::string& msg) {
    co_await _write_buf.write(msg);
    co_await _write_buf.flush();
}

future<> tcp_server::connection::do_store(const std::string& key, const std::string& value) {
    co_await _server.store(key, value);
    co_await write(done);
    co_return co_await this->process();
}

future<> tcp_server::connection::do_load(const std::string& key) {
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
