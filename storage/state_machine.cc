#include "state_machine.h"

namespace lightkv {

Error LightKV::start(const std::string& conf) {
    Error error;
    butil::EndPoint addr(butil::my_ip(), FLAGS_port);
    braft::NodeOptions node_options;
    if (conf != " ") {
        if (node_options.initial_conf.parse_from(conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration \'" << conf << '\'';
            error.set_error_message("Fail to parse configuration \'" + conf + '\'');
            error.set_error_code(-1);
            return error;
        }
    }
    node_options.election_timeout_ms = FLAGS_election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = FLAGS_snapshot_interval;
    std::string prefix = "local://" + FLAGS_store_path + "/" + std::to_string(FLAGS_port)
                             + "/" + std::to_string(shard_id) + "/raft";
    node_options.log_uri = prefix + "/log";
    node_options.raft_meta_uri = prefix + "/raft_meta";
    node_options.snapshot_uri = prefix + "/snapshot";
    node_options.disable_cli = FLAGS_disable_cli;
    braft::Node* node = new braft::Node(FLAGS_group + "_" + std::to_string(shard_id), braft::PeerId(addr, shard_id));
    if (node->init(node_options) != 0) {
        LOG(ERROR) << "Fail to init raft node";
        error.set_error_message("Fail to init raft node");
        error.set_error_code(-1);
        delete node;
        return error;
    }
    _node.reset(node);
    error.set_error_code(0);
    return error;
}

void LightKV::operate(const LightKVRequest* request,
                       LightKVResponse* response,
                       ::google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    // Serialize request to the replicated write-ahead-log so that all the
    // peers in the group receive this request as well.
    // Notice that _value can't be modified in this routine otherwise it
    // will be inconsistent with others in this group.
    const int64_t term = _leader_term.load(butil::memory_order_relaxed);
    if (term < 0) {
        response->set_redirect(redirect().c_str());
        response->mutable_error()->set_error_code(-1);
        response->mutable_error()->set_error_message("The address is not leader.");
        return ;
    }
    if (request->operator_type() == SELECT) {
        select(request, response, done);
    } else {
        butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message("Fail to serialize request");
            return;
        }
        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or
        // fail
        task.done = new LightKVClosure(this, request, response,
                                            done_guard.release());

        if (FLAGS_check_term) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
        _node->apply(task);
    }
}

void LightKV::select(const LightKVRequest* request,
                       LightKVResponse* response,
                       ::google::protobuf::Closure* done)
{
    LOG(WARNING) << "receive a select request";
    // This is the leader and is up-to-date. It's safe to respond client
    response->mutable_error()->CopyFrom(_store->select(request->key(), response->mutable_value()));
}

void LightKV::on_apply(braft::Iterator& iter)
{
    // A batch of tasks are committed, which must be processed through 
    // |iter|
    for (; iter.valid(); iter.next()) {
        // This guard helps invoke iter.done()->Run() asynchronously to
        // avoid that callback blocks the StateMachine.
        braft::AsyncClosureGuard closure_guard(iter.done());
        if (iter.done()) {
            LightKVClosure* c = dynamic_cast<LightKVClosure*>(iter.done());
            auto request = c->request();
            switch (request->operator_type()) {
                case INSERT:
                    c->response()->mutable_error()->CopyFrom(_store->insert(request->key(), request->value()));
                    break;
                case DELETE:
                    c->response()->mutable_error()->CopyFrom(_store->delete_(request->key()));
                    break;
            }
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
            LightKVRequest request;
            CHECK(request.ParseFromZeroCopyStream(&wrapper));
            switch (request.operator_type()) {
                case INSERT:
                    _store->insert(request.key(), request.value());
                    break;
                case DELETE:
                    _store->delete_(request.key());
                    break;
            }
        }
    }
}

void LightKVClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<LightKVClosure> self_guard(this);
    // Respond this RPC.
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    // Try redirect if this request failed.
    LOG(ERROR) << "1";
    _response->set_redirect(_kv_store->redirect());
    LOG(ERROR) << "2";
    _response->mutable_error()->set_error_code(-1);
    LOG(ERROR) << "3";
    _response->mutable_error()->set_error_message("The address maybe is not leader.");
    LOG(ERROR) << "4";
}

void LightKV::on_snapshot_save(::braft::SnapshotWriter* writer,
                                  ::braft::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    std::string snapshot_path = writer->get_path() + "/rocksdb";
    LOG(INFO) << "Saving snapshot to " << snapshot_path;
    Error error = _store->do_checkpoint(snapshot_path);
    if (error.error_code() != 0) {
        LOG(ERROR) << error.error_message();
        return ;
    }
    std::vector<std::string> files;
    get_files(snapshot_path, files);
    for (std::string& file : files) {
        writer->add_file("rocksdb/"+ file);
    }
}

int LightKV::on_snapshot_load(::braft::SnapshotReader* reader)
{
    CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
    std::vector<std::string> files;
    std::string snapshot_path = reader->get_path();
    reader->list_files(&files);
    for (auto& file : files) {
        file = snapshot_path + "/" + file;
    }
    Error error = _store->read_snapshot(files);
    if (error.error_code() != 0) {
        LOG(ERROR) << error.error_message();
    }
    return 0;
}

} // namespace lightkv