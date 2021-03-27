#include <fstream>
#include <vector>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include<unistd.h>
namespace lightkv
{
typedef int ShardID;
int copy_file(const char *source_file, const char *new_file);
void get_files(const std::string& path, std::vector<std::string>& files);
bool does_dir_exist(const std::string& path);
} // namespace lightkv

