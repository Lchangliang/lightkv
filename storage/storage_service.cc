#include "storage_service.h"

namespace lightkv {

void StorageServiceImpl::init_store(::google::protobuf::RpcController* controller,
                       const ::lightkv::InitStoreRequest* request,
                       ::lightkv::InitStoreResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    LOG(INFO) << "receive a init_store request, shard id: " + std::to_string(request->shard_id());
    std::string endpoints;
    for (int i = 0; i < request->peers_size(); i++) {
        std::string tmp;
        endpoint_to_string(request->peers(i), &tmp);
        endpoints += tmp + ","; 
    }
    LOG(INFO) << "Peers: " + endpoints;
    std::unique_lock<std::shared_mutex> lock(storage_map->_mutex);
    ShardID shard_id = request->shard_id();
    if (storage_map->kv_stores.find(shard_id) != storage_map->kv_stores.end()) {
        response->mutable_error()->set_error_message("The storage already has shard: " + std::to_string(shard_id));
        response->mutable_error()->set_error_code(-1);
        return ;
    }
    std::string path = FLAGS_store_path + "/" + std::to_string(FLAGS_port) + "/" + std::to_string(shard_id);
    mkdir(path.c_str(), 0755);
    std::shared_ptr<StoreInterface> store(new RocksDBStoreImpl(shard_id));
    LightKV kv_store(store, shard_id);
    std::string conf = "";
    for (int i = 0; i < request->peers_size(); i++) {
        const lightkv::EndPoint& peer = request->peers(i);
        conf += peer.ip() + ":" + std::to_string(peer.port()) + ":" + std::to_string(shard_id) + ",";
    }
    storage_map->kv_stores.insert(std::make_pair(shard_id, kv_store));
    Error error = storage_map->kv_stores.find(shard_id)->second.start(conf);
    response->mutable_error()->CopyFrom(error);
    if (error.error_code() != 0) {
        storage_map->kv_stores.erase(shard_id);
    }
}

void StorageServiceImpl::delete_store(::google::protobuf::RpcController* controller,
                       const ::lightkv::DeleteStoreRequest* request,
                       ::lightkv::DeleteStoreResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::unique_lock<std::shared_mutex> lock(storage_map->_mutex);
    ShardID shard_id = request->shard_id();
    if (storage_map->kv_stores.find(shard_id) != storage_map->kv_stores.end()) {
        std::string path = FLAGS_store_path + "/" + std::to_string(FLAGS_port)
                             + "/" + std::to_string(shard_id);
        storage_map->kv_stores.erase(shard_id);
        rm_dir(path.c_str());
    }
}


} // namespace lightkv