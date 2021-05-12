#ifndef LIGHTKV_PROXY_SERVICE
#define LIGHTKV_PROXY_SERVICE

#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include <set>
#include <string>
#include "shard_manager.h"
#include "interface/lightkv.pb.h"
#include "util/util.h"

DECLARE_string(group);
DECLARE_int32(timeout_ms);

namespace lightkv {

class ProxyServiceImpl : public ProxyService
{
public:
    ProxyServiceImpl(int shard_count, int replica) {
        shard_manager.set_config(shard_count, replica);
        shard_manager.start();
    }

    void insert(::google::protobuf::RpcController* controller,
                       const ::lightkv::InsertRequest* request,
                       ::lightkv::InsertResponse* response,
                       ::google::protobuf::Closure* done);

    void select(::google::protobuf::RpcController* controller,
                       const ::lightkv::SelectRequest* request,
                       ::lightkv::SelectResponse* response,
                       ::google::protobuf::Closure* done);

    void delete_(::google::protobuf::RpcController* controller,
                       const ::lightkv::DeleteRequest* request,
                       ::lightkv::DeleteResponse* response,
                       ::google::protobuf::Closure* done);

    void get_shards_state(::google::protobuf::RpcController* controller,
                       const ::lightkv::ShardsStateRequest* request,
                       ::lightkv::ShardsStateResponse* response,
                       ::google::protobuf::Closure* done);
    
    void heart_beat(::google::protobuf::RpcController* controller,
                       const ::lightkv::HeartBeatRequest* request,
                       ::lightkv::HeartBeatResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        shard_manager.receive_heartBeat(request);
    }
private:
    void transfer_from_insert_request(LightKVRequest* storage_request, const InsertRequest* request);
    void transfer_to_insert_response(const LightKVResponse& storage_response, InsertResponse* response);
    void transfer_from_select_request(LightKVRequest* storage_request, const SelectRequest* request);
    void transfer_to_select_response(const LightKVResponse& storage_response, SelectResponse* response);
    void transfer_from_delete_request(LightKVRequest* storage_request, const DeleteRequest* request);
    void transfer_to_delete_response(const LightKVResponse& storage_response, DeleteResponse* response);
    ShardManager shard_manager;
};
} // namespace lightkv
#endif