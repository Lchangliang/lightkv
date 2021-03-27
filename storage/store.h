#ifndef LIGHTKV_STORE
#define LIGHTKV_STORE

#include <vector>
#include "interface/lightkv.pb.h"

namespace lightkv {

class StoreInterface {
public:
    virtual ~StoreInterface() {};
    virtual Error insert(const std::string&, const std::string&) = 0; 
    virtual Error delete_(const std::string&) = 0;
    virtual Error select(const std::string&, std::vector<std::pair<std::string, std::string>>*) = 0;
    virtual Error select_prefix(const std::string&, std::vector<std::pair<std::string, std::string>>*) = 0;
    virtual Error select_range(const std::string&, const std::string&, std::vector<std::pair<std::string, std::string>>*) = 0;
    virtual Error do_checkpoint(const std::string&) = 0;
    virtual Error read_snapshot(const std::vector<std::string>&) = 0;
};
} // namespace LightKV

#endif
