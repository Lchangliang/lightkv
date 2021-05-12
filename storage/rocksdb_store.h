#ifndef LIGHTKV_ROCKSDB_STORE
#define LIGHTKV_ROCKSDB_STORE

#include <cassert>
#include <unistd.h>
#include <vector>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/checkpoint.h>
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include "util/util.h"
#include "store.h"

DECLARE_string(store_path);
DECLARE_int32(port);

namespace lightkv {
class RocksDBStoreImpl : public StoreInterface {
public:
    RocksDBStoreImpl(ShardID shard_id) : db(nullptr) {
        _rocksdb_path = FLAGS_store_path + "/" + std::to_string(FLAGS_port)
                             + "/" + std::to_string(shard_id) + "/rocksdb";
        init_rocksdb();
    }
    ~RocksDBStoreImpl() { delete db; }
    Error insert(const std::string&, const std::string&);
    Error delete_(const std::string&);
    Error select(const std::string&, std::string*);
    Error do_checkpoint(const std::string&);
    Error read_snapshot(const std::vector<std::string>&);
private:
    void init_rocksdb() {
        LOG(INFO) << "init rocksdb";
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, _rocksdb_path, &db);
        assert(status.ok());   
    }
    std::string _rocksdb_path;
    rocksdb::DB* db;
};
} // namespace LightKV
#endif