#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "client.h"

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        zNode* server = nullptr;

        v_mutex.lock();
        
        
        //if the server already exists
        if (!clusters[serverinfo->serverid()-1].empty()) { //serverid currently holds cluster id
            std::cout << "Server " << serverinfo->serverid() << " heartbeat..." << std::endl;
            zNode* server = clusters[serverinfo->serverid()-1][0];
            //if the server missed a hearbeat
            if (server->missed_heartbeat) {
                //set that it has not missed a heartbeat
                std::cout << "Server " << serverinfo->serverid() << " reconnected..." << std::endl;
                server->missed_heartbeat = false;
            }
            //set last heartbeat to now
            server->last_heartbeat = getTimeNow();
        }
        //else, need to create new server in list
        else {
            //create new z node with server info and insert it into cluster
            std::cout << "New server, id: " << serverinfo->serverid() << std::endl;
            zNode* newServer = new zNode;
            newServer->serverID = 1; //SERVID
            newServer->hostname = serverinfo->hostname();
            newServer->port = serverinfo->port();
            newServer->missed_heartbeat = false;
            clusters[serverinfo->serverid()-1].push_back(newServer);
        }

        v_mutex.unlock();

        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int clusterId = ((id->id() - 1) % 3 ); //took out the +1 as I'll be using it as an index

        std::cout << "Client " << id->id() << " being routed to cluster " << clusterId << std::endl;


        v_mutex.lock();

        if (clusters[clusterId].empty()) {
            std::cerr << "Error: requested server not found" << std::endl;
            serverinfo->set_serverid(-1);
            v_mutex.unlock();
            return Status::OK;
        }

        zNode* z = clusters[clusterId][0];
        //if server isn't active, return a -1
        if (z == nullptr || !z->isActive()) {
            serverinfo->set_serverid(-1);
        }
        //if server is active, reply with its values
        else {
            serverinfo->set_serverid(1);
            serverinfo->set_hostname(z->hostname);
            serverinfo->set_port(z->port);
            //serverinfo->set_type(z->type); this is defaulted to server for mp 2.1
        }

        v_mutex.unlock();

        std::cout << "completed GetServer" << std::endl;


        return Status::OK;
    }


};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("localhost:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "9090";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            for(auto& s : c){
                if(difftime(getTimeNow(),s->last_heartbeat)>10){
                    std::cout << "missed heartbeat from server w/ id 1... " << std::endl;
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

