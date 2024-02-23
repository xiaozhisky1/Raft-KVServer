#include "util.h"
#include <cstdio>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <cstdarg>

void myAssert(bool condition, std::string message) {
    if (!condition) {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}


std::chrono::_V2::system_clock::time_point now() {
    return std::chrono::high_resolution_clock::now();
}

std::chrono::milliseconds getRandomizedElectionTimeout() {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

    return std::chrono::milliseconds(dist(rng));
}

void sleepNMilliseconds(int N) {
    std::this_thread::sleep_for(std::chrono::milliseconds(N));
};


bool getReleasePort(short &port) {
    short num = 0;
    while (!isReleasePort(port) && num < 30) {
        ++port;
        ++num;
    }
    if (num >= 30) {
        port = -1;
        return false;
    }
    return true;
}

bool isReleasePort(unsigned short usPort) {
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(usPort);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int ret = ::bind(s, (sockaddr *) &addr, sizeof(addr));
    if (ret != 0) {
        close(s);
        return false;
    }
    close(s);
    return true;
}

void DPrintf(const char *format, ...) {
    if (Debug) {
        // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
        time_t now = time(nullptr);
        tm *nowtm = localtime(&now);
        va_list args;
        va_start(args, format);
        std::printf("[%d-%d-%d-%d-%d-%d] ", nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                    nowtm->tm_min, nowtm->tm_sec);
        std::vprintf(format, args);
        std::printf("\n");
        va_end(args);
    }
}
// 将文件转化为字符串
std::string fileToString(std::string path){
    std::filesystem::path file_path(path);

    if (!std::filesystem::exists(file_path)) {
        std::cout << path << " 不存在" << std::endl;
        return nullptr;
    }

    std::ifstream ifs;
    ifs.open(path, std::ios::binary); // 以二进制模式打开文件

    if (!ifs.is_open()) {
        std::cout << "文件打开失败" << std::endl;
        return nullptr;
    }

    // 获取文件长度
    ifs.seekg(0, std::ios::end);
    std::size_t length = ifs.tellg();
    ifs.seekg(0, std::ios::beg);
    std::cout << "文件长度: " << length << " 字节" << std::endl;

    // 创建字符串缓冲区
    std::string file_data(length, '\0');

    // 读取二进制数据到字符串缓冲区
    ifs.read(&file_data[0], length);

    // 关闭文件
    ifs.close();

    return file_data;
}

bool savefile(std::string path, std::string &content){
    std::ofstream ofs(path, std::ios::binary); // 创建并打开文件，以输出模式写入

    if (!ofs.is_open()) {
        std::cout << "文件创建失败" << std::endl;
        return false;
    }

    // 将二进制数据写入文件
    ofs.write(content.c_str(), content.size());

    // 关闭文件
    ofs.close();

    std::cout << "数据已成功写入 " << path << std::endl;

    return true;
}
// 删除文件
bool deletefile(const std::string& filename){
    int result = std::remove(filename.c_str());
    if (result == 0) {
        std::cout << "文件删除成功！" << std::endl;
        return true;
    } else {
        std::cerr << "文件删除失败！" << std::endl;
        return false;
    }
}