#ifndef LIGHTKV_HEARTBEAT
#define LIGHTKV_HEARTBEAT

#include <bthread/bthread.h>
#include <brpc/channel.h>
#include "util/util.h"
#include "storage_service.h"
#include "interface/lightkv.pb.h"

DECLARE_string(ip);
DECLARE_int32(port);
DECLARE_string(proxy_ip);
DECLARE_int32(proxy_port);

namespace lightkv {

class HeartBeat {
public:
    HeartBeat(lightkv::StorageMap* storage_map)
        : ip(FLAGS_ip), port(FLAGS_port), 
            proxy_ip(FLAGS_proxy_ip), proxy_port(FLAGS_proxy_port), 
            storage_map(storage_map) {}

    void start();
    
private:
    static void* _start(void*);

    std::string ip;
    int port;
    std::string proxy_ip;
    int proxy_port;
    lightkv::StorageMap* storage_map;
};

} // namespace lightkv
#endif