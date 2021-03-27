#include "proxy_service.h"

namespace lightkv
{

void ProxyServiceImpl::insert(::google::protobuf::RpcController* controller,
                       const ::lightkv::InsertRequest* request,
                       ::lightkv::InsertResponse* response,
                       ::google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    for (;;) { 
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                            FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                response->mutable_error()->set_error_code(-1);
                response->mutable_error()->set_error_message("Fail to refresh_leader");
                break ;
            }
            continue ;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message("network error: Fail to init channel");
            break ;
        }
        StorageService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        lightkv::LightKVRequest storage_request;
        lightkv::LightKVResponse storage_response;
        transfer_from_insert_request(&storage_request, request);
        stub.operate(&cntl, &storage_request, &storage_response, NULL);
        transfer_to_insert_response(storage_response, response);
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                        << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message(
                    "Fail to send request to " + leader.to_string() + " : " + cntl.ErrorText());
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue ;
        }
        if (response->error().error_code() == -1) {
            LOG(WARNING) << "Fail to send request to " << leader
                        << ", redirecting to "
                        << (response->redirect() != "" 
                                ? response->redirect() : "nowhere");
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
    for (;;) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                            FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                response->mutable_error()->set_error_code(-1);
                response->mutable_error()->set_error_message("Fail to refresh_leader");
                break ;
            }
            continue ;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message("network error: Fail to init channel");
            break ;
        }
        StorageService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        lightkv::LightKVRequest storage_request;
        lightkv::LightKVResponse storage_response;
        transfer_from_select_request(&storage_request, request);
        stub.operate(&cntl, &storage_request, &storage_response, NULL);
        transfer_to_select_response(storage_response, response);
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                        << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message(
                    "Fail to send request to " + leader.to_string() + " : " + cntl.ErrorText());
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue ;
        }
        if (response->error().error_code() == -1) {
            LOG(WARNING) << "Fail to send request to " << leader
                        << ", redirecting to "
                        << (response->redirect() != "" 
                                ? response->redirect() : "nowhere");
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
    for (;;) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                            FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                response->mutable_error()->set_error_code(-1);
                response->mutable_error()->set_error_message("Fail to refresh_leader");
                break ;
            }
            continue ;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message("network error: Fail to init channel");
            break ;
        }
        StorageService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        lightkv::LightKVRequest storage_request;
        lightkv::LightKVResponse storage_response;
        transfer_from_delete_request(&storage_request, request);
        stub.operate(&cntl, &storage_request, &storage_response, NULL);
        transfer_to_delete_response(storage_response, response);
        if (cntl.Failed()) {
            LOG(WARNING) << "Fail to send request to " << leader
                        << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            response->mutable_error()->set_error_code(-3);
            response->mutable_error()->set_error_message(
                    "Fail to send request to " + leader.to_string() + " : " + cntl.ErrorText());
            braft::rtb::update_leader(FLAGS_group, braft::PeerId());
            bthread_usleep(FLAGS_timeout_ms * 1000L);
            continue ;
        }
        if (response->error().error_code() == -1) {
            LOG(WARNING) << "Fail to send request to " << leader
                        << ", redirecting to "
                        << (response->redirect() != "" 
                                ? response->redirect() : "nowhere");
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
    storage_request->set_lkey(request->key());
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
    storage_request->set_select_type(request->type());
    storage_request->set_lkey(request->lkey());
    storage_request->set_rkey(request->rkey());
}

void ProxyServiceImpl::transfer_to_select_response(const LightKVResponse& storage_response, SelectResponse* response)
{
    response->mutable_error()->CopyFrom(storage_response.error());
    response->set_redirect(storage_response.redirect());
    int length = storage_response.keys_size();
    for (int i = 0; i < length; i++) {
        response->add_keys(storage_response.keys(i));
        response->add_values(storage_response.values(i));
    }
}

void ProxyServiceImpl::transfer_from_delete_request(LightKVRequest* storage_request, const DeleteRequest* request)
{
    storage_request->set_operator_type(DELETE);
    storage_request->set_lkey(request->key());
}

void ProxyServiceImpl::transfer_to_delete_response(const LightKVResponse& storage_response, DeleteResponse* response)
{
    response->mutable_error()->CopyFrom(storage_response.error());
    response->set_redirect(storage_response.redirect());
}
} // namespace lightkv  