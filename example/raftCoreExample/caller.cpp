#include "clerk.h"
#include <iostream>
#include "util.h"
int main(){
    Clerk client;
    client.Init("test.conf");
    auto start = now();

    while(true){
        std::string input;
        std::cout << "Enter a command: ";
        std::getline(std::cin, input);

        std::string command;
        std::vector<std::string> arguments;
        // 分割输入字符串，以空格为分隔符
        size_t pos = 0;
        while ((pos = input.find(' ')) != std::string::npos) {
            std::string token = input.substr(0, pos);
            if (command.empty()) {
                command = token;
            } else {
                arguments.push_back(token);
            }
            input.erase(0, pos + 1);
        }

        // 处理最后一个参数
        if (!input.empty()) {
            arguments.push_back(input);
        }

        if(command == "upload"){
             if(arguments.size()!=1){
                cout<<"参数错误"<<endl;
                continue;
            }
            string s = fileToString(arguments[0]);
            int i;
            for (i = arguments[0].size() - 1; arguments[0][i] != '/'; i--)
                ;
            string arg = arguments[0].substr(i + 1, arguments[0].size() - i);
            client.Upload(arg, s);
            cout<<arguments[0]<<" 已上传"<<endl;
        }
        else if(command == "download"){
            std::string get;
            if (arguments.size() == 0 || arguments.size() > 2)
            {
                cout << "参数错误" << endl;
                continue;
            }
            client.Download(arguments[0],get);
            if(arguments.size()==1){
                savefile(arguments[0], get);
            }
            else if(arguments.size() == 2)
                savefile("./" + arguments[1], get);
        }
        else if(command == "ls"){
            // if (arguments.size() != 0)
            // {
            //     cout << "参数错误" << endl;
            //     continue;
            // }
            std::string ls;
            client.Ls("", ls);
            cout<<ls<<endl;
        }
        else if(command == "delete"){
            if(arguments.size() != 1 ){
                cout<<"参数错误"<<endl;
                continue;
            }
            string res;
            client.Delete(arguments[0], res);
            if(res.size()>0) cout<<res<<endl;
            else cout<<"删除成功"<<endl;
        }
        else cout<<"command not found"<<endl;
    }
    //string s = fileToString("./test.txt");// /home/rookie/KVstorageBaseRaft-cpp/images/img.png
    //client.Upload("test.txt", s);
    // string ls;
    // client.Ls("",ls);
    // cout<<ls<<endl;

    // sleep(2);
    // std::string get;
    // client.Download("test.txt",get);
    // cout<<"得到结果,即将存储"<<endl;
    // savefile("./output.test", get);
    // cout<<"获取到了test.txt"<<endl;


    // int count = 500;
    // int tmp = count;
    // while (tmp --){
    //     client.Put("x",std::to_string(tmp));

    //     std::string get1 = client.Get("x");
    //     std::printf("get return :{%s}\r\n",get1.c_str());
    // }
    return 0;
}