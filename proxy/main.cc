#include <brpc/server.h>           // brpc::Server
#include "proxy_service.h"


DEFINE_int32(timeout_ms, 1000, "Timeout for each request");
DEFINE_string(group, "Lightkv", "Id of the replication group");
DEFINE_int32(port, 7100, "Listen port of this peer");
DEFINE_string(path, "/tmp/proxy", "proxy config");
DEFINE_int32(shard_count, 128, "shard number");
DEFINE_int32(replica, 3, "replica");

static void* start_service(void* arg) 
{
    std::vector<int>* config = (std::vector<int>*) arg;
    // Generally you only need one Server.
    brpc::Server server;
    lightkv::ProxyServiceImpl proxy_service(config->at(0), config->at(1));
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

void read_config(std::vector<int>* result) {
    rocksdb::DB *db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, FLAGS_path + "_" + std::to_string(FLAGS_port), &db);
    // TODO 读取配置
    std::string shard_count;
    status = db->Get(rocksdb::ReadOptions(), "shardCount", &shard_count);
    if (status.code() == 1) { // kNotFound
        shard_count = std::to_string(FLAGS_shard_count);
        db->Put(rocksdb::WriteOptions(), "shardCount", shard_count);
    } else if (status.code() != 0) {
        exit(1);
    }
    result->push_back(atoi(shard_count.c_str()));
    std::string replica;
    status = db->Get(rocksdb::ReadOptions(), "replica", &replica);
    if (status.code() == 1) {
        replica = std::to_string(FLAGS_replica);
        db->Put(rocksdb::WriteOptions(), "replica", replica);
    } else if (status.code() != 0) {
        exit(1);
    }
    result->push_back(atoi(replica.c_str()));
    db->Close();
    delete db;
}

int main(int argc, char* argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;
    // Register configuration of target group to RouteTable
    std::vector<int> config;
    read_config(&config);
    bthread_t tid;
    if (bthread_start_background(&tid, NULL, start_service, &config) != 0) {
        LOG(ERROR) << "Fail to create bthread";
        return -1;
    }
    while (!brpc::IsAskedToQuit()) {
        sleep(5);
    }
    bthread_join(tid, NULL);
}