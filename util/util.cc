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

void string_to_endpoint(const std::string& address, EndPoint* endpoint) {
    int limit = 0;
    for (int i = 0; i < address.size(); i++) {
        if (address[i] == ':') {
            limit = i;
            break;
        }
    }
    endpoint->set_ip(address.substr(0, limit));
    int port = atoi(address.substr(limit+1, (address.size()-limit-1)).c_str());
    endpoint->set_port(port);
}

void endpoint_to_string(const EndPoint& endpoint, std::string* address) {
    address->assign(endpoint.ip() + ":" + std::to_string(endpoint.port()));
}

int rm_dir(std::string dir_full_path) {
    DIR* dirp = opendir(dir_full_path.c_str());
    if(!dirp)
    {
        return -1;
    }
    struct dirent *dir;
    struct stat st;
    while((dir = readdir(dirp)) != NULL)
    {
        if(strcmp(dir->d_name,".") == 0
           || strcmp(dir->d_name,"..") == 0)
        {
            continue;
        }
        std::string sub_path = dir_full_path + '/' + dir->d_name;
        if(lstat(sub_path.c_str(),&st) == -1)
        {
            //Log("rm_dir:lstat ",sub_path," error");
            continue;
        }
        if(S_ISDIR(st.st_mode))
        {
            if(rm_dir(sub_path) == -1) // 如果是目录文件，递归删除
            {
                closedir(dirp);
                return -1;
            }
            rmdir(sub_path.c_str());
        }
        else if(S_ISREG(st.st_mode))
        {
            unlink(sub_path.c_str());     // 如果是普通文件，则unlink
        }
        else
        {
            //Log("rm_dir:st_mode ",sub_path," error");
            continue;
        }
    }
    if(rmdir(dir_full_path.c_str()) == -1)//delete dir itself.
    {
        closedir(dirp);
        return -1;
    }
    closedir(dirp);
    return 0;
}

} // namespace lightkv
