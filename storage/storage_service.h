#ifndef LIGHTKV_STORAGE_SERVER
#define LIGHTKV_STORAGE_SERVER

#include <vector>
#include <gflags/gflags.h>              // DEFINE_*
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <butil/hash.h>
#include "interface/lightkv.pb.h"
#include "store.h"
#include "rocksdb_store.h"
#include "util/util.h"

DECLARE_string(store_path);
DECLARE_bool(check_term);
DECLARE_bool(disable_cli);
DECLARE_bool(log_applied_task);
DECLARE_int32(election_timeout_ms);
DECLARE_int32(port);
DECLARE_int32(snapshot_interval);
DECLARE_string(conf);
DECLARE_string(raft_data_path);
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
    LightKV(StoreInterface* store): 
        _node(NULL), 
        _leader_term(0), 
        _store(store) {}
    ~LightKV() {
        delete _node;
    }
    // Starts this node
    int start();

    void operate(const LightKVRequest* request,
                       LightKVResponse* response,
                       ::google::protobuf::Closure* done);

    bool is_leader() const 
    { return _leader_term.load(butil::memory_order_acquire) > 0; }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
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
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
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

    void get_raft_snapshot_files(const std::string& snapshot_path, 
            std::vector<std::string>& external_files);

private:
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    StoreInterface* _store;
};

class StorageServiceImpl : public StorageService {
public:
    explicit StorageServiceImpl(LightKV* kv_store) : kv_store(kv_store) {}
    void operate(::google::protobuf::RpcController* controller,
                       const ::lightkv::LightKVRequest* request,
                       ::lightkv::LightKVResponse* response,
                       ::google::protobuf::Closure* done) {
        kv_store->operate(request, response, done);
    }
private:
    LightKV *kv_store;
};
} // namespace LightKV
#endif