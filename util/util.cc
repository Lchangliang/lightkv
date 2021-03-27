#include "util.h"

namespace lightkv {
int copy_file(const char *source_file, const char *new_file)
{
    std::ifstream in;
    std::ofstream out;
    in.open(source_file, std::ios::binary);
    if(in.fail()) {
        in.close();
        out.close();
        return 0;
    }
    out.open(new_file, std::ios::binary);
    if(out.fail()) {
        out.close();
        in.close();
        return 0;
    } else {
        out << in.rdbuf();
        out.close();
        in.close();
        return 1;
    }
}

void get_files(const std::string& path, std::vector<std::string>& files)
{
    if (path.empty()) {
        return ;
    }
    struct stat s;
    stat(path.c_str(), &s);
    if (!S_ISDIR(s.st_mode)) {
        return ;
    }
    DIR* open_dir = opendir(path.c_str());
    if (NULL == open_dir) {
        std::exit(EXIT_FAILURE);
    }
    dirent* p = nullptr;
    while((p = readdir(open_dir)) != nullptr) {
        struct stat st;
        if (p->d_name[0] != '.') {
            std::string file_name(p->d_name);
            files.push_back(file_name);
        }
    }
    closedir(open_dir);
    return ;
}   
bool does_dir_exist(const std::string& path) {
    return access(path.c_str(), F_OK) == 0;
}
} // namespace lightkv
