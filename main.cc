#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/net/api.hh>

#include "server.hh"

using namespace seastar;

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
