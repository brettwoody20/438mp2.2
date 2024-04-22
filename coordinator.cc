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
using csce438::HBResponse;
using csce438::Users;

struct zNode{
    int clusterID; 
    int machineID;
    bool master;
    std::string hostname;
    std::string port;
    std::string synch_hostname;
    std::string synch_port;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
};


//storage of server lists
std::mutex cluster_mutex;

std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};

//storage of cluster masters
std::mutex master_mutex;

std::vector<zNode*> masters = {nullptr, nullptr, nullptr};


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

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, HBResponse* hbresponse) override {
        //New functionality
            //if cluster is empty, add server and set it as master (set master to true and add to master vertex)
            //if cluster is not empty and doesn't contain the server
                //add the server, set master as false
            //if cluster is not empty and contains the machineID, 
                //maintain heartbeat
                //return synchronizer and slave IP address (if exist)

        zNode* server = nullptr;

        cluster_mutex.lock();

        bool check1 = !clusters[serverinfo->clusterid()-1].empty();
        bool check2 = clusterContains(serverinfo->clusterid()-1, serverinfo->machineid());

        
        
        //if cluster is not empty and contains the machineID
        if (check1 && check2) { //serverid currently holds cluster id
            std::cout << "Cluster" << serverinfo->clusterid() << " Server" << serverinfo->machineid() << " heartbeat1..." << std::endl;
            zNode* server = clusters[serverinfo->clusterid()-1][0];
            //if the server missed a hearbeat
            if (server->missed_heartbeat) {
                //set that it has not missed a heartbeat
                std::cout << "Server " << serverinfo->clusterid() << " reconnected..." << std::endl;
                server->missed_heartbeat = false;
            }
            //set last heartbeat to now
            server->last_heartbeat = getTimeNow();

            zNode* curr = getNode(serverinfo->clusterid(), serverinfo->machineid());

            hbresponse->set_master(curr->master);
            hbresponse->set_synchport(curr->synch_hostname);
            if (curr->master) {
                hbresponse->set_slavehostname(clusters[curr->clusterID-1][curr->machineID]->hostname);
                hbresponse->set_slaveport(clusters[curr->clusterID-1][curr->machineID]->port);
            } else {
                hbresponse->set_slavehostname("null");
                hbresponse->set_slaveport("null");
            }

            cluster_mutex.unlock();
        }
        //if cluster is not empty and doesn't contain the server, add it as a slave
        else if (check1) {
            //create new z node with server info and insert it into cluster
            std::cout << "New slave server, cluster" << serverinfo->clusterid() << " server" << serverinfo->machineid() << std::endl;

            zNode* newServer = new zNode;
            newServer->clusterID = serverinfo->clusterid();
            newServer->machineID = serverinfo->machineid();
            newServer->master = false;
            newServer->hostname = serverinfo->hostname();
            newServer->port = serverinfo->port();
            newServer->synch_hostname = "null";
            newServer->synch_port = "null";
            newServer->missed_heartbeat = false;

            clusters[serverinfo->clusterid()-1].push_back(newServer);

            cluster_mutex.unlock();

            std::string newDir = "data/cluster" + std::to_string(newServer->clusterID) + "/machine" + std::to_string(newServer->machineID);
            std::filesystem::create_directories(newDir);
        }
        //eif the cluster is empty, add it as a master
        else {
            //create new z node with server info and insert it into cluster
            std::cerr << "Empty:" << check1 << " Contained:" << check2 << std::endl;
            std::cout << "New master server, cluster" << serverinfo->clusterid() << " server" << serverinfo->machineid() << std::endl;


            zNode* newServer = new zNode;
            newServer->clusterID = serverinfo->clusterid();
            newServer->machineID = serverinfo->machineid();
            newServer->master = true;
            newServer->hostname = serverinfo->hostname();
            newServer->port = serverinfo->port();
            newServer->synch_hostname = "null";
            newServer->synch_port = "null";
            newServer->missed_heartbeat = false;

            clusters[newServer->clusterID].push_back(newServer);

            cluster_mutex.unlock();

            master_mutex.lock();
            masters[newServer->clusterID] = newServer;
            master_mutex.unlock();

            std::string newDir = "data/cluster" + std::to_string(newServer->clusterID) + "/machine" + std::to_string(newServer->machineID);
            std::filesystem::create_directories(newDir);
        }


        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        //ToDo: make sure everything is up to date

        int clusterId = ((id->id() - 1) % 3 ); //took out the +1 as I'll be using it as an index

        std::cout << "Client " << id->id() << " being routed to cluster " << clusterId << std::endl;


        cluster_mutex.lock();

        if (clusters[clusterId].empty()) {
            std::cerr << "Error: requested server not found" << std::endl;
            serverinfo->set_machineid(-1);
            cluster_mutex.unlock();
            return Status::OK;
        }

        zNode* z = clusters[clusterId][0];
        //if server isn't active, return a -1
        if (z == nullptr || !z->isActive()) {
            serverinfo->set_machineid(-1);
        }
        //if server is active, reply with its values
        else {
            serverinfo->set_machineid(1);
            serverinfo->set_hostname(z->hostname);
            serverinfo->set_port(z->port);
            //serverinfo->set_type(z->type); this is defaulted to server for mp 2.1
        }

        cluster_mutex.unlock();

        std::cout << "completed GetServer" << std::endl;


        return Status::OK;
    }

    Status registerSynch(ServerContext* context, const ServerInfo* serverInfo, ID* id) {
        //passed ip, port, and cluster of synchronizer process

        //assigns it to a pre-existing znode/server

        //based on this server it calculates the machine number number and returns that in id
        
        return Status::OK;
    }

    Status getSynchs(ServerContext* context, const Users* users, ServerList* serverlist) {

        //passed a list of userIDs

        //creates an empty list of hostnames and ports (see server List function)

        //for each id

            //calculate the cluster that the id is in

            //add the IP address of each synchronizer in that cluster to the lists

        //insert those lists into serverlist (again see server's List function)

        return Status::OK;
    }

    Status getAllSynchs (ServerContext* context, const Confirmation* confrimation, ServerList* ServerList) {

        //for each cluster
            //for each machine
                //add the synch hostname and port to serverList (See Server's List function)

        return Status::OK;
    }

    private:

    bool clusterContains(int clusterIndex, int id) {
        for (zNode* z : clusters[clusterIndex]) {
            if (z->machineID == id) {
                return true;
            }
        }
        return false;
    }

    zNode* getNode(int cluster_id, int machine_id) {
        for (zNode* z : clusters[cluster_id-1]) {
            if (z->machineID == machine_id) {
                return z;
            }
        }
        return nullptr;
    }



};

void RunServer(std::string port_no){
    //create directories
    std::string clust1 = "data/cluster1";
    std::string clust2 = "data/cluster2";
    std::string clust3 = "data/cluster3";
    std::filesystem::create_directories(clust1);
    std::filesystem::create_directories(clust2);
    std::filesystem::create_directories(clust3);
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
    //NEW FUNCTIONALITY: 
    // if it has been more than 10 seconds since last heartbeat and it has already missed one
        //don't set last heartbeat
        //if master
            //make not master
            //make next server in cluster master

    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true
        // Your code below

        cluster_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            for(auto& s : c){
                if(difftime(getTimeNow(),s->last_heartbeat)>10){
                    std::cout << "missed heartbeat from cluster" << s->clusterID << " server" << s->machineID << std::endl;
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        cluster_mutex.unlock();

        sleep(10);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

