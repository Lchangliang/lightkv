#ifndef LIGHTKV_STORAGE_SERVER
#define LIGHTKV_STORAGE_SERVER

#include <unordered_map>
#include <shared_mutex>
#include <gflags/gflags.h>              // DEFINE_*
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include "interface/lightkv.pb.h"
#include "rocksdb_store.h"
#include "state_machine.h"
#include "store.h"

DECLARE_string(store_path);

namespace lightkv {

struct StorageMap {
    std::shared_mutex _mutex;
    std::unordered_map<ShardID, LightKV> kv_stores;
};

class StorageServiceImpl : public StorageService {
public:
    explicit StorageServiceImpl(StorageMap* storage_map) 
            :   storage_map(storage_map) {}

    void start_raft_service() {
        std::unique_lock<std::shared_mutex> lock(storage_map->_mutex);
        std::vector<std::string> shard_ids;
        get_files(FLAGS_store_path + "/" + std::to_string(FLAGS_port), shard_ids);
        for (std::string& shard_id : shard_ids) {
            int shard = atoi(shard_id.c_str());
            std::shared_ptr<StoreInterface> store(new RocksDBStoreImpl(shard));
            storage_map->kv_stores.insert(std::make_pair(shard, LightKV(store, shard)));
        }
        for (auto& pair : storage_map->kv_stores) {
            pair.second.start();
        }
    }

    void operate(::google::protobuf::RpcController* controller,
                       const ::lightkv::LightKVRequest* request,
                       ::lightkv::LightKVResponse* response,
                       ::google::protobuf::Closure* done) {
        std::shared_lock<std::shared_mutex> lock(storage_map->_mutex);
        ShardID shard_id = request->shard_id();
        if (storage_map->kv_stores.find(shard_id) != storage_map->kv_stores.end()) {
            storage_map->kv_stores.find(shard_id)->second.operate(request, response, done);
        }
    }

    void init_store(::google::protobuf::RpcController* controller,
                       const ::lightkv::InitStoreRequest* request,
                       ::lightkv::InitStoreResponse* response,
                       ::google::protobuf::Closure* done);
    
    void delete_store(::google::protobuf::RpcController* controller,
                       const ::lightkv::DeleteStoreRequest* request,
                       ::lightkv::DeleteStoreResponse* response,
                       ::google::protobuf::Closure* done);

private:
    StorageMap* storage_map;
};
} // namespace LightKV
#endif