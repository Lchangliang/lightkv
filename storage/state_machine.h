#ifndef LIGHTKV_STATE_MACHINE
#define LIGHTKV_STATE_MACHINE

#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <brpc/server.h>           // brpc::Server
#include "interface/lightkv.pb.h"
#include "store.h"
#include "util/util.h"

DECLARE_string(store_path);
DECLARE_bool(check_term);
DECLARE_bool(disable_cli);
DECLARE_bool(log_applied_task);
DECLARE_int32(election_timeout_ms);
DECLARE_int32(port);
DECLARE_int32(snapshot_interval);
DECLARE_string(group);

namespace lightkv {
class LightKV;

class LightKVClosure : public braft::Closure {
public:
    LightKVClosure(LightKV* kv_store, 
                    const LightKVRequest* request,
                    LightKVResponse* response,
                    google::protobuf::Closure* done)
        : _kv_store(kv_store)
        , _request(request)
        , _response(response)
        , _done(done) {}
    ~LightKVClosure() {}

    const LightKVRequest* request() const { return _request; }
    LightKVResponse* response() const { return _response; }
    void Run();

private:
    LightKV* _kv_store;
    const LightKVRequest* _request;
    LightKVResponse* _response;
    google::protobuf::Closure* _done;
};

class LightKV : public braft::StateMachine {
public:

    LightKV(std::shared_ptr<StoreInterface>& store, ShardID shard_id): 
        _node(NULL), 
        _leader_term(0), 
        _store(store), 
        shard_id(shard_id){}

    LightKV(const lightkv::LightKV& lightkv) {
        _node = lightkv._node;
        _leader_term.store(lightkv._leader_term, butil::memory_order_release);
        _store = lightkv._store;
        shard_id = lightkv.shard_id;
    }

    // Starts this node
    Error start(const std::string& conf = "");

    void operate(const LightKVRequest* request,
                       LightKVResponse* response,
                       ::google::protobuf::Closure* done);

    bool is_leader() const 
    { 
        return _leader_term.load(butil::memory_order_acquire) > 0; 
    }
    
private:
friend class LightKVClosure;
    void select(const LightKVRequest* request,
                    LightKVResponse* response,
                    ::google::protobuf::Closure* done);
    std::string redirect() {
        if (_node) {
            braft::PeerId leader = _node->leader_id();
            if (!leader.is_empty()) {
                return leader.to_string();
            }
        }
        return "";
    }

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter);

    void on_snapshot_save(::braft::SnapshotWriter* writer,
                                  ::braft::Closure* done);

    int on_snapshot_load(::braft::SnapshotReader* reader);
    
    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node " + std::to_string(shard_id) + " becomes leader, term: " + std::to_string(_leader_term.load(butil::memory_order_acquire));
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node " + std::to_string(shard_id) +" is down";
    }

    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Meta raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }
    // end of @braft::StateMachine

private:
    std::shared_ptr<braft::Node> _node;
    butil::atomic<int64_t> _leader_term;
    std::shared_ptr<StoreInterface> _store;
    ShardID shard_id;
};

} // namespace lightkv
#endif