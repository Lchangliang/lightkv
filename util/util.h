#ifndef LIGHTKV_UTIL
#define LIGHTKV_UTIL

#include <fstream>
#include <vector>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include<unistd.h>
#include "interface/lightkv.pb.h"

#define HEARTBEAT_INTERVAL 5
#define HEARTBEAT_TIMEOUT 30
#define SHARD_MANAGER_INTERVAL 15

namespace lightkv
{
typedef int ShardID;
int copy_file(const char *source_file, const char *new_file);
void get_files(const std::string& path, std::vector<std::string>& files);
bool does_dir_exist(const std::string& path);
void string_to_endpoint(const std::string& address, EndPoint* endpoint);
void endpoint_to_string(const EndPoint& endpoint, std::string* address);
int rm_dir(std::string dir_full_path);
} // namespace lightkv

#endif

