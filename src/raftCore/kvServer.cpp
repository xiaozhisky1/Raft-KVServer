#include "kvServer.h"

#include <rpcprovider.h>

#include "mprpcconfig.h"
#include <iostream>
#include <filesystem>
using namespace std;
void KvServer::DprintfKVDB() {
    if (!Debug) {
        return;
    }
    std::lock_guard<std::mutex> lg(m_mtx);
    DEFER {
        // for (const auto &item: m_kvDB) {
        //     DPrintf("[DBInfo ----]Key : %s, Value : %s", &item.first, &item.second);
        // }
        m_skipList.display_list();
    };

}

void KvServer::ExecuteUploadOpOnKVDB(Op op) {
    //if op.IfDuplicate {   //get请求是可重复执行的，因此可以不用判复
    //	return
    //}
    m_mtx.lock();

    //m_skipList.insert_set_element(op.Key,op.Value);

    // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
    //     m_kvDB[op.Key] = m_kvDB[op.Key] + op.Value;
    // } else {
    //     m_kvDB.insert(std::make_pair(op.Key, op.Value));
    // }
    m_file.insert(op.Key);
    // 将文件存入
    savefile("/home/rookie/fs/" + op.Key, op.Value);

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();


    //    DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId, op.Key, op.Value)
    //DprintfKVDB();
}
// 此处为download命令
void KvServer::ExecuteGetOpOnKVDB(Op op, std::string &value, bool *exist) {
    m_mtx.lock();
    *exist = false;
    // if(m_skipList.search_element(op.Key, *value)) {
    //     *exist = true;
    //     // *value = m_skipList.se //value已经完成赋值了
    // }
    // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
    //     *exist = true;
    //     *value = m_kvDB[op.Key];
    // }
    if(m_file.find(op.Key) != m_file.end()){
        *exist = true;
        // 从 /home/rookie/fs下读取文件文件
        value = fileToString("/home/rookie/fs/" + op.Key);
        // std::cout<<"------------------------------------"<<endl;
        // std::cout<<"---------已读取到待下载文件---------"<<endl;
        // std::cout<<"------------------------------------"<<endl;
        // std::cout<<"---------已读取到待下载文件---------"<<endl;
    }

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();


    if (*exist) {
        //                DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId, op.RequestId, op.Key, value)
    } else {
        //        DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId, op.RequestId, op.Key)
    }
    //DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key,op.Value);
    // m_kvDB[op.Key] = op.Value;

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();


    //    DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId, op.Key, op.Value)
    //DprintfKVDB();
}

// 处理ls命令
void KvServer::ExecuteLsOpOnKVDB(Op op, std::string *value){
    m_mtx.lock();
    *value = "";
    for(auto it : m_file){
        *value += it + " "; 
    }
    m_mtx.unlock();
}

// 处理delete命令
void KvServer::ExecuteDeleteOpOnKVDB(Op op,std::string *errtype){
    m_mtx.lock();
    *errtype = "";
    if(m_file.find(op.Key) == m_file.end()){
        *errtype = "file not exit";
    }
    else {
        std::string filename = "/home/rookie/fs/" + op.Key;
        int result = std::remove(filename.c_str());
        m_file.erase(op.Key);
        if(result != 0) 
            *errtype = "delete fail";
    }
    m_mtx.unlock();
}
// 删除文件


// 处理来自clerk的Get RPC
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();


    int raftIndex = -1;
    int _ = -1;
    bool isLeader = false;
    m_raftNode->Start(op, &raftIndex, &_, &isLeader); //raftIndex：raft预计的logIndex ，虽然是预计，但是正确情况下是准确的，op的具体内容对raft来说 是隔离的

    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }


    // create waitForCh
    m_mtx.lock();

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];

    m_mtx.unlock(); //直接解锁，等待任务执行完成，不能一直拿锁等待


    // timeout
    Op raftCommitOp;

    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        //        DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)
        int _ = -1;
        bool isLeader = false;
        m_raftNode->GetState(&_, &isLeader);

        if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
            //如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
            // 不会违反线性一致性
            std::string value;
            bool exist = false;
            if(op.Operation == "download"){// 这也是download命令
                ExecuteGetOpOnKVDB(op, value, &exist);// 真正执行Get操作
                if (exist) {
                    reply->set_err(OK);
                    reply->set_value(value);
                } else {
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            }
            else if(op.Operation == "ls"){
                ExecuteLsOpOnKVDB(op, &value);
                reply->set_err(OK);
                reply->set_value(value);
            }
            else if(op.Operation == "delete"){
                std::string errtype;
                ExecuteDeleteOpOnKVDB(op, &errtype);
                if(errtype == ""){
                    reply->set_err(OK);
                    reply->set_value("");
                }
                else{
                    reply->set_err(OK);
                    reply->set_value(errtype);
                }
            }
            
        } else {
            reply->set_err(ErrWrongLeader); //返回这个，其实就是让clerk换一个节点重试
        }
    } else {
        //raft已经提交了该command（op），可以正式开始执行了
        //        DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
        //todo 这里还要再次检验的原因：目前未知，如果没有这里的代码，处理读取相关的指令会失败
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            std::string value;
            bool exist = false;
                if(op.Operation == "download"){// 这也是download命令
                ExecuteGetOpOnKVDB(op, value, &exist);// 真正执行Get操作
                if (exist) {
                    reply->set_err(OK);
                    reply->set_value(value);
                } else {
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            }
            else if(op.Operation == "ls"){
                ExecuteLsOpOnKVDB(op, &value);
                reply->set_err(OK);
                reply->set_value(value);
            }
            else if(op.Operation == "delete"){
                std::string errtype;
                ExecuteDeleteOpOnKVDB(op, &errtype);
                if(errtype == ""){
                    reply->set_err(OK);
                    reply->set_value("");
                }
                else{
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            }
        } else {
            reply->set_err(ErrWrongLeader);
            //            DPrintf("[GET ] 不满足：raftCommitOp.ClientId{%v} == op.ClientId{%v} && raftCommitOp.RequestId{%v} == op.RequestId{%v}", raftCommitOp.ClientId, op.ClientId, raftCommitOp.RequestId, op.RequestId)
        }
    }
    m_mtx.lock(); //todo 這個可以先弄一個defer，因爲刪除優先級並不高，先把rpc發回去更加重要
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
    Op op;
    op.parseFromString(message.Command);


    DPrintf(
        "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, Opreation {%s}, Key :{%s}, Value :{%s}",
        m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
        return;
    }

    // State Machine (KVServer solute the duplicate problem)
    // duplicate command will not be exed
    if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
        // execute command
        if (op.Operation == "Put") {
            ExecutePutOpOnKVDB(op);
        }
        if (op.Operation == "upload") {
            ExecuteUploadOpOnKVDB(op);
        }
        //  kv.lastRequestId[op.ClientId] = op.RequestId  在Executexxx函数里面更新的
    }
    //到这里kvDB已经制作了快照
    if (m_maxRaftState != -1) {
        IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
        //如果raft的log太大（大于指定的比例）就把制作快照
    }

    // Send message to the chan of op.ClientId
    SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
        return false;
        // todo :不存在这个client就创建
    }
    return RequestId <= m_lastRequestId[ClientId];
}

//get和put//append執行的具體細節是不一樣的
//PutAppend 在收到 Raft 通过applyChan发来的消息之后执行，具体函数内部只判断幂等性（即是否重复）。
//而 get 函数在收到 Raft 消息之前就可以执行，因为 get 无论是否重复都可以再次执行。
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();
    int raftIndex = -1;
    int _ = -1;
    bool isleader = false;

    m_raftNode->Start(op, &raftIndex, &_, &isleader);// 给raft节点发送日志


    if (!isleader) {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but not leader",
            m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

        reply->set_err(ErrWrongLeader);// 将错误信息发回给clerk，其得知后会跟换raft节点
        return;
    }
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is leader ",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
    m_mtx.lock();
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {//如果队列中不存在与 raftIndex 对应的通道
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));//创建一个新的通道并插入到 waitApplyCh 中
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];// 获取与 raftIndex 对应的通道。


    m_mtx.unlock(); //直接解锁，等待任务执行完成，不能一直拿锁等待

    // timeout
    Op raftCommitOp;

    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {//通过超时pop来限定命令执行时间，如果超过时间还没拿到消息说明命令执行超时了。
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s"
            , m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            reply->set_err(OK); // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
        } else {// 超时了，且不是重复的请求
            reply->set_err(ErrWrongLeader); ///这里返回这个的目的让clerk重新尝试
        }
    } else {//没超时，命令可能真正的在raft集群执行成功了。
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s"
            , m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
        if (raftCommitOp.ClientId == op.ClientId &&
            raftCommitOp.RequestId == op.RequestId) {
            //可能发生leader的变更导致日志被覆盖，因此必须检查
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }

    m_mtx.lock();

    auto tmp = waitApplyCh[raftIndex];//创建一个指向 waitApplyCh[raftIndex] 的临时指针 tmp
    waitApplyCh.erase(raftIndex);// 从 waitApplyCh 中删除与 raftIndex 对应的通道。
    delete tmp;// 删除之前创建的通道。
    m_mtx.unlock();
}

void KvServer::ReadRaftApplyCommandLoop() {
    while (true) {
        //如果只操作applyChan不用拿锁，因为applyChan自己带锁
        auto message = applyChan->Pop(); //阻塞弹出，返回弹出的message（当applyChan为空时就阻塞住）
        DPrintf("---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
                m_me);
        // listen to every command applied by its raft ,delivery to relative RPC Handler

        if (message.CommandValid) {
            GetCommandFromRaft(message);
        }
        if (message.SnapshotValid) {
            GetSnapShotFromRaft(message);
        }
    }
}

//raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
// 关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
// 因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
// snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
    if (snapshot.empty()) {
        // bootstrap without any state?
        return;
    }
    parseFromString(snapshot);

    //    r := bytes.NewBuffer(snapshot)
    //    d := labgob.NewDecoder(r)
    //
    //    var persist_kvdb map[string]string  //理应快照
    //    var persist_lastRequestId map[int64]int //快照这个为了维护线性一致性
    //
    //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
    //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
    //        } else {
    //        kv.kvDB = persist_kvdb
    //        kv.lastRequestId = persist_lastRequestId
    //    }
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId {%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        return false;
    }
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId {%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
        // Send SnapShot Command
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
    }
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

std::string KvServer::MakeSnapShot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
    KvServer::PutAppend(request, response);
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
    // KvServer::Get(request, response);
    // done->Run();
        std::thread([=,this](){
        KvServer::Get(request, response);
        done->Run();
    }).detach();
}
KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port):
m_skipList(6){
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me); //创建一个共享指针 persister，用于管理持久化数据。

    m_me = me;
    m_maxRaftState = maxraftstate;

    applyChan = std::make_shared<LockQueue<ApplyMsg> >();//共享队列：kvServer和raft节点的通信管道

    m_raftNode = std::make_shared<Raft>();//创建一个 Raft 节点对象 m_raftNode
    ////////////////clerk层面 kvserver开启rpc接受功能
    //    同时raft与raft节点之间也要开启rpc功能，因此有两个注册
    std::thread t([this, port]()-> void {
        // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
        RpcProvider provider;
        // 由于KvServer继承自 raftKVRpcProctoc::kvServerRpc，因此可以直接读取到该对象的描述信息
        provider.NotifyService(this);// 将当前 KvServer 对象和 m_raftNode 对象注册到 RPC 服务中。
        provider.NotifyService(this->m_raftNode.get()); //todo：这里获取了原始指针，后面检查一下有没有泄露的问题 或者 shareptr释放的问题
        // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
        provider.Run(m_me, port);
    });
    t.detach();

    ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
    ////这里使用睡眠来保证
    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6);
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
    //获取所有raft节点ip、port ，并进行连接  ,要排除自己, 这一段和Clerk.init()基本一致
    MprpcConfig config;
    config.LoadConfigFile(nodeInforFileName.c_str());
    std::vector<std::pair<std::string, short> > ipPortVt;
    for (int i = 0; i < INT_MAX - 1; ++i) {
        std::string node = "node" + std::to_string(i);

        std::string nodeIp = config.Load(node + "ip");
        std::string nodePortStr = config.Load(node + "port");
        if (nodeIp.empty()) {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str())); //沒有atos方法，可以考慮自己实现
    }
    std::vector<std::shared_ptr<RaftRpcUtil> > servers;
    //进行连接
    for (int i = 0; i < ipPortVt.size(); ++i) {
        if (i == m_me) {
            servers.push_back(nullptr);
            continue;
        }
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);// 为每个节点创建一个 raftServerRpcUtil 对象,raftServerRpcUtil 是一个用于与 Raft 节点通信的工具类
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

        std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
    }
    sleep(ipPortVt.size() - me); //等待所有节点相互连接成功，再启动raft
    m_raftNode->init(servers, m_me, persister, applyChan);// 将Raft节点初始化
    //kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

    //////////////////////////////////

    // You may need initialization code here.
    // m_kvDB; //kvdb初始化
    m_skipList;
    waitApplyCh;
    m_lastRequestId;
    m_file;
    for (const auto & entry : filesystem::directory_iterator("/home/rookie/fs")) { // 初始化 m_file
        if (filesystem::is_regular_file(entry.path())) {
            this->m_file.insert(entry.path().filename());
            //std::cout << entry.path().filename() << std::endl;
        }
    }
    m_lastSnapShotRaftLogIndex = 0; //todo:感覺這個函數沒什麼用，不如直接調用raft節點中的snapshot值？？？
    auto snapshot = persister->ReadSnapshot();
    if (!snapshot.empty()) {
        ReadSnapShotToInstall(snapshot);
    }
    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this); //线程不断读取applyChan中的message，有message就立刻执行对应的操作
    t2.join(); //由於ReadRaftApplyCommandLoop一直不會結束，相當於一直卡死在這裏了
}
