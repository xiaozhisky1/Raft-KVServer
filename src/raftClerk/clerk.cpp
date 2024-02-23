//
// Created by swx on 23-6-4.
//
#include "clerk.h"

#include "raftServerRpcUtil.h"

#include "util.h"

#include <string>
#include <vector>
std::string Clerk::Get(std::string key, std::string op, std::string& value) {
    m_requestId++;// 增加 m_requestId，表示请求的唯一标识
    auto requestId = m_requestId;//设置请求的客户端ID和请求ID。
    int server = m_recentLeaderId;
    raftKVRpcProctoc::GetArgs args;// key,clientid,requestid是GetArgs中的值，proto对这些值提供set方法
    args.set_key(key);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);
    args.set_op(op);

    while (true) {
        raftKVRpcProctoc::GetReply reply;// m_servers 是一个服务器列表
        bool ok = m_servers[server]->Get(&args, &reply); // 向当前服务器发送 Get 请求，并获取响应。
        if (!ok || reply.err() == ErrWrongLeader) {//会一直重试，因为requestId没有改变，因此可能会因为RPC的丢失或者其他情况导致重试，kvserver层来保证不重复执行（线性一致性）
            server = (server + 1) % m_servers.size();// 如果请求失败或者响应表明当前服务器不是领导者（ErrWrongLeader），则切换到下一个服务器继续尝试。
            continue;
        }
        if(reply.err() == ErrNoKey){// 如果响应表明键不存在（ErrNoKey），则返回空字符串。
            return "";
        }
        if(reply.err() == OK){// 如果响应正常（OK），更新最近的领导者ID，并返回响应中的值。
            m_recentLeaderId = server;// m_recentLeaderId 是最近的领导者服务器的ID
            value = reply.value();
            return "";
        }
    }
    return "";
}
// 向服务器追加或设置键值对，代码结构与Get()类似
void Clerk::PutAppend(std::string key, std::string value, std::string op) {
    // You will have to modify this function.
    m_requestId++;
    auto requestId = m_requestId;
    auto server = m_recentLeaderId;
    while (true){
        raftKVRpcProctoc::PutAppendArgs args;
        args.set_key(key);args.set_value(value);args.set_op(op);args.set_clientid(m_clientId);args.set_requestid(requestId);
        raftKVRpcProctoc::PutAppendReply reply;
        bool ok = m_servers[server]->PutAppend(&args,&reply);
        if(!ok || reply.err()==ErrWrongLeader){

            DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}",server,server+1,op.c_str());
            if(!ok){
                DPrintf("重试原因 ，rpc失敗 ，");
            }
            if(reply.err()==ErrWrongLeader){
                DPrintf("重試原因：非leader");
            }
            server = (server+1)%m_servers.size();  // try the next server
            continue;
        }
        if(reply.err()==OK){  //什么时候reply errno为ok呢？？？
            m_recentLeaderId = server;
            return ;
        }
    }
}

void Clerk::Put(std::string key, std::string value) {
    PutAppend(key, value, "Put");
}

void Clerk::Upload(std::string key, std::string value) {
    PutAppend(key, value, "upload");
}
std::string Clerk::Download(std::string key, std::string& value){
    return Get(key,"download", value);
}
std::string Clerk::Delete(std::string key, std::string& value){
    return Get(key, "delete",value);
}
std::string Clerk::Ls(std::string key, std::string& value){
    return Get(key, "ls",value);
}
//初始化客户端
void Clerk::Init(std::string configFileName) {// 接收一个字符串类型的 configFileName，表示配置文件的名称。
    //获取所有raft节点ip、port ，并进行连接
    MprpcConfig config;// 创建一个 MprpcConfig 对象 config，用于加载配置文件。
    config.LoadConfigFile(configFileName.c_str());
    std::vector<std::pair<std::string,short>> ipPortVt;
    for (int i = 0; i < INT_MAX - 1 ; ++i) {
        std::string node = "node" + std::to_string(i);

        std::string nodeIp = config.Load(node+"ip");
        std::string nodePortStr = config.Load(node+"port");
        if(nodeIp.empty()){
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));   //沒有atos方法，可以考慮自己实现
    }
    //进行连接
    for (const auto &item:ipPortVt){// 遍历 ipPortVt，为每个节点创建一个 raftServerRpcUtil 对象，并将其添加到 m_servers 中。
        std::string ip = item.first; short port = item.second;
        //2024-01-04 todo：bug fix
        auto* rpc = new raftServerRpcUtil(ip,port);// 为每个节点创建一个 raftServerRpcUtil 对象,raftServerRpcUtil 是一个用于与 Raft 节点通信的工具类
        m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));// 添加到 m_servers 中
    }
}

Clerk::Clerk() :m_clientId(Uuid()),m_requestId(0),m_recentLeaderId(0){

}

