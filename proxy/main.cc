#include <brpc/server.h>           // brpc::Server
#include "proxy_service.h"


DEFINE_int32(timeout_ms, 1000, "Timeout for each request");
DEFINE_string(conf, "", "Configuration of the raft group");
DEFINE_string(group, "Lightkv", "Id of the replication group");
DEFINE_int32(port, 7100, "Listen port of this peer");

static void* start_service(void* arg) 
{
    // Generally you only need one Server.
    brpc::Server server;
    lightkv::ProxyServiceImpl proxy_service;
    // Add your service into RPC server
    if (server.AddService(&proxy_service, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return nullptr;
    }

    // It's recommended to start the server before Counter is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return nullptr;
    }

    server.RunUntilAskedToQuit();
    return nullptr;
}

int main(int argc, char* argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;
    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    bthread_t tid;
    if (bthread_start_background(&tid, NULL, start_service, NULL) != 0) {
        LOG(ERROR) << "Fail to create bthread";
        return -1;
    }
    while (!brpc::IsAskedToQuit()) {
        sleep(5);
    }
    bthread_join(tid, NULL);
}