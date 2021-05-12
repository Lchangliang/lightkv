#include "proxy_service.h"

namespace lightkv
{

void ProxyServiceImpl::insert(::google::protobuf::RpcController* controller,
                       const ::lightkv::InsertRequest* request,
                       ::lightkv::InsertResponse* response,
                       ::google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    std::hash<std::string> hash_str;
    ShardID shard_id = hash_str(request->key()) % shard_manager.get_shard_count();
    LOG(INFO) << "key: " + request->key() + " shard_id: " + std::to_string(shard_id);
    for (;;) { 
        EndPoint leader;
        string_to_endpoint(shard_manager.get_leader(shard_id), &leader);
        brpc::Channel channel;
        if (leader.ip() == "0") {
            response->mutable_error()->set_error_code(-1);
            response->mutable_error()->set_error_message("Fail to find leader");
            break ;
        } else {
            if (channel.Init(leader.ip().c_str(), leader.port(), NULL) != 0) {
                LOG(ERROR) << "Fail to init channel to " << leader.ip() + ":" + std::to_string(leader.port());
                response->mutable_error()->set_error_code(-3);
                response->mutable_error()->set_error_message("network error: Fail to init channel");
                break ;
            }
        }
        
        StorageService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        lightkv::LightKVRequest storage_request;
        storage_request.set_shard_id(shard_id);
        lightkv::LightKVResponse storage_response;
        transfer_from_insert_request(&storage_request, request);
        stub.operate(&cntl, &storage_request, &storage_response, NULL);
        transfer_to_insert_response(storage_response, response);
        if (cntl.Failed()) {
            // Clear leadership since this RPC failed.
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message(
                    "Fail to send request to " + leader.ip() + ":" + std::to_string(leader.port()) + " : " + cntl.ErrorText());
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue ;
        }
        if (response->error().error_code() == -1) {
            // Update route table since we have redirect information
            braft::rtb::update_leader(FLAGS_group, response->redirect());
            continue ;
        }
        break ;
    }
}

void ProxyServiceImpl::select(::google::protobuf::RpcController* controller,
                       const ::lightkv::SelectRequest* request,
                       ::lightkv::SelectResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::hash<std::string> hash_str;
    ShardID shard_id = hash_str(request->key()) % shard_manager.get_shard_count();
    LOG(INFO) << "key: " + request->key() + " shard_id: " + std::to_string(shard_id);
    for (;;) {
        EndPoint leader;
        string_to_endpoint(shard_manager.get_leader(shard_id), &leader);
        LOG(INFO) << shard_manager.get_leader(shard_id);
        brpc::Channel channel;
        if (leader.ip() == "0") {
            response->mutable_error()->set_error_code(-1);
            response->mutable_error()->set_error_message("Fail to find leader");
            break ;
        } else {
            if (channel.Init(leader.ip().c_str(), leader.port(), NULL) != 0) {
                LOG(ERROR) << "Fail to init channel to " << leader.ip() + ":" + std::to_string(leader.port());
                response->mutable_error()->set_error_code(-3);
                response->mutable_error()->set_error_message("network error: Fail to init channel");
                break ;
            }
        }
        StorageService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        lightkv::LightKVRequest storage_request;
        storage_request.set_shard_id(shard_id);
        lightkv::LightKVResponse storage_response;
        transfer_from_select_request(&storage_request, request);
        stub.operate(&cntl, &storage_request, &storage_response, NULL);
        transfer_to_select_response(storage_response, response);
        if (cntl.Failed()) {
            // Clear leadership since this RPC failed.
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message(
                    "Fail to send request to " + leader.ip() + ":" + std::to_string(leader.port()) + " : " + cntl.ErrorText());
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue ;
        }
        if (response->error().error_code() == -1) {

            // Update route table since we have redirect information
            braft::rtb::update_leader(FLAGS_group, response->redirect());
            continue ;
        }
        break ;
    }
}

void ProxyServiceImpl::delete_(::google::protobuf::RpcController* controller,
                       const ::lightkv::DeleteRequest* request,
                       ::lightkv::DeleteResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::hash<std::string> hash_str;
    ShardID shard_id = hash_str(request->key()) % shard_manager.get_shard_count();
    for (;;) {
        EndPoint leader;
        string_to_endpoint(shard_manager.get_leader(shard_id), &leader);
        brpc::Channel channel;
        if (leader.ip() == "0") {
            response->mutable_error()->set_error_code(-1);
            response->mutable_error()->set_error_message("Fail to find leader");
            break ;
        } else {
            if (channel.Init(leader.ip().c_str(), leader.port(), NULL) != 0) {
                LOG(ERROR) << "Fail to init channel to " << leader.ip() + ":" + std::to_string(leader.port());
                response->mutable_error()->set_error_code(-3);
                response->mutable_error()->set_error_message("network error: Fail to init channel");
                break ;
            }
        }

        StorageService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        lightkv::LightKVRequest storage_request;
        storage_request.set_shard_id(shard_id);
        lightkv::LightKVResponse storage_response;
        transfer_from_delete_request(&storage_request, request);
        stub.operate(&cntl, &storage_request, &storage_response, NULL);
        transfer_to_delete_response(storage_response, response);
        if (cntl.Failed()) {
            // Clear leadership since this RPC failed.
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message(
                    "Fail to send request to " + leader.ip() + ":" + std::to_string(leader.port()) + " : " + cntl.ErrorText());
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue ;
        }
        if (response->error().error_code() == -1) {
            // Update route table since we have redirect information
            braft::rtb::update_leader(FLAGS_group, response->redirect());
            continue ;
        }
        break ;
    }
}

void ProxyServiceImpl::transfer_from_insert_request(LightKVRequest* storage_request, const InsertRequest* request)
{
    storage_request->set_operator_type(INSERT);
    storage_request->set_key(request->key());
    storage_request->set_value(request->value());
}

void ProxyServiceImpl::transfer_to_insert_response(const LightKVResponse& storage_response, InsertResponse* response)
{
    response->mutable_error()->CopyFrom(storage_response.error());
    response->set_redirect(storage_response.redirect());
}

void ProxyServiceImpl::transfer_from_select_request(LightKVRequest* storage_request, const SelectRequest* request)
{
    storage_request->set_operator_type(SELECT);
    storage_request->set_key(request->key());
}

void ProxyServiceImpl::transfer_to_select_response(const LightKVResponse& storage_response, SelectResponse* response)
{
    response->mutable_error()->CopyFrom(storage_response.error());
    response->set_redirect(storage_response.redirect());
    response->set_value(storage_response.value());
}

void ProxyServiceImpl::transfer_from_delete_request(LightKVRequest* storage_request, const DeleteRequest* request)
{
    storage_request->set_operator_type(DELETE);
    storage_request->set_key(request->key());
}

void ProxyServiceImpl::transfer_to_delete_response(const LightKVResponse& storage_response, DeleteResponse* response)
{
    response->mutable_error()->CopyFrom(storage_response.error());
    response->set_redirect(storage_response.redirect());
}

void ProxyServiceImpl::get_shards_state(::google::protobuf::RpcController* controller,
                       const ::lightkv::ShardsStateRequest* request,
                       ::lightkv::ShardsStateResponse* response,
                       ::google::protobuf::Closure* done) 
{
    brpc::ClosureGuard done_guard(done);
    for (int i = 0; i < shard_manager.shards.size(); i++) {
        ShardState ss;
        ss.set_shard_id(i);
        auto tablets = shard_manager.shards[i];
        for (Tablet tablet : tablets) {
            if (tablet.is_leader) {
                string_to_endpoint(tablet.address, ss.mutable_leader());
            } else {
                string_to_endpoint(tablet.address, ss.add_followers());
            }
        }
        response->add_shards_state()->CopyFrom(ss);
    }
}

} // namespace lightkv  