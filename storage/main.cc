#include "storage_service.h"

DEFINE_string(store_path, "/tmp/rocksdb_tmp", "rocksdb data path");
DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(group, "Lightkv", "Id of the replication group");
DEFINE_string(raft_data_path, "/tmp/raft_tmp", "raft data path");

int main(int argc, char *argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;

    lightkv::StoreInterface *_store =  new lightkv::RocksDBStoreImpl(FLAGS_store_path);
    lightkv::LightKV kv_store(_store);
    lightkv::StorageServiceImpl service(&kv_store);

    // Add your service into RPC server
    if (server.AddService(&service, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Counter is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start LightKV;
    if (kv_store.start() != 0) {
        LOG(ERROR) << "Fail to start LightKV";
        return -1;
    }

    LOG(INFO) << "LightKV service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "LightKV service is going to quit";

    // Stop lightkv before server
    kv_store.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    kv_store.join();
    server.Join();
    delete _store;
    return 0;
}