//
// Created by swx on 23-6-4.
//
#include "clerk.h"
#include <iostream>
#include "util.h"
int main(){
    Clerk client;
    client.Init("test.conf");
    auto start = now();
    int count = 500;
    int tmp = count;
    // while (tmp --){
    //     client.Put("x",std::to_string(tmp));

    //     std::string get1 = client.Get("x");
    //     std::printf("get return :{%s}\r\n",get1.c_str());
    // }

    // string put = fileToString("../images/XV6.png");// "../images/img.png"
    // client.Put("x",put);
    while(true){
        string get;
        client.Get("myfile.txt",get); // （Get的第一个参数没有起到实际用途，真正Get的文件地址在KvServer::ExecuteGetOpOnKVDB()）
        savefile("./success0.jpg",get);
        sleep(2);
    }
   
    return 0;
}