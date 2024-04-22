    #include <bits/fs_fwd.h>
    #include <ctime>

    #include <google/protobuf/timestamp.pb.h>
    #include <google/protobuf/duration.pb.h>
    #include <chrono>
    #include <sys/stat.h>
    #include <sys/types.h>
    #include <vector>
    #include <unordered_set>
    #include <filesystem>
    #include <fstream>
    #include <iostream>
    #include <memory>
    #include <string>
    #include <thread>
    #include <mutex>
    #include <stdlib.h>
    #include <unistd.h>
    #include <algorithm>
    #include <google/protobuf/util/time_util.h>
    #include <grpc++/grpc++.h>

    #include "sns.grpc.pb.h"
    #include "sns.pb.h"
    #include "coordinator.grpc.pb.h"
    #include "coordinator.pb.h"


    namespace fs = std::filesystem;

    using google::protobuf::Timestamp;
    using google::protobuf::Duration;
    using grpc::Server;
    using grpc::ClientContext;
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
    using csce438::Users;
    using csce438::ServerList;
    using csce438::SynchService;
    using csce438::Post;

    int clusterID; //calculated from syncID
    int machineID; //returned from coord

    int synchID = 1;
    std::vector<std::string> get_lines_from_file(std::string);
    void run_synchronizer(std::string,std::string,std::string,int);
    std::vector<std::string> get_all_users_func(int);
    std::vector<std::string> get_tl_or_fl(int, int, bool);

    std::unique_ptr<csce438::CoordService::Stub> coordinator_stub;

    class SynchServiceImpl final : public SynchService::Service {

        /*ToDo:
        -passed a new client, add it to the list of clients in machine directory
        -add "u1" as "1" in file
        */
        Status newClientSynch (ServerContext* context, const ID* id, Confirmation* confirmation) {

            return Status::OK;
        }

        /*ToDo:
        -passed u1, u2 where u1 is following u2
        -add u1 to u2 list of followers in u2/followers.txt
        */
        Status newFollowSynch (ServerContext* context, const Users* users, Confirmation* confirmation) {

            return Status::OK;
        }

        /*ToDo:
        -passed a post from u1
        -for all users, ux, that follow u1, add it to their timeline: ux/timeline.txt
        */
        Status newPostSynch (ServerContext* context, const Post* post, Confirmation* confirmation) {
            
            return Status::OK;
        }

        /*ToDo:
        -passed u1 as new user
        -call coord rpc getAllSynchs
        -call rpc newClientSynch on all servers returned
        */
        Status newClientServ (ServerContext* context, const ID* id, Confirmation* confirmation) {

            return Status::OK;
        }

        /*ToDo:
        -passed u1, u2 where u1 is following u2
        -call coord rpc getSynchs and pass u2
        -call rpc newFollowSynch on the servers returned
        */
        Status newFollowServ (ServerContext* context, const Users* users, Confirmation* confirmation) {

            return Status::OK;
        }

        /*ToDo:
        -passed a post from u1
        -gets list of followers from u1/followers.txt
        -calls coord rpc getSynchs and pass list of followers
        -calls newPostSynch on all the servers returned
        */
        Status newPostServ (ServerContext* context, const Post* post, Confirmation* confirmation) {
            
            return Status::OK;
        }


        // Status GetAllUsers(ServerContext* context, const Confirmation* confirmation, AllUsers* allusers) override{
        //     //std::cout<<"Got GetAllUsers"<<std::endl;
        //     std::vector<std::string> list = get_all_users_func(synchID);
        //     //package list
        //     for(auto s:list){
        //         allusers->add_users(s);
        //     }

        //     //return list
        //     return Status::OK;
        // }

        // Status GetTLFL(ServerContext* context, const ID* id, TLFL* tlfl){
        //     //std::cout<<"Got GetTLFL"<<std::endl;
        //     int clientID = id->id();

        //     std::vector<std::string> tl = get_tl_or_fl(synchID, clientID, true);
        //     std::vector<std::string> fl = get_tl_or_fl(synchID, clientID, false);

        //     //now populate TLFL tl and fl for return
        //     for(auto s:tl){
        //         tlfl->add_tl(s);
        //     }
        //     for(auto s:fl){
        //         tlfl->add_fl(s);
        //     }
        //     tlfl->set_status(true); 

        //     return Status::OK;
        // }

        // Status ResynchServer(ServerContext* context, const ServerInfo* serverinfo, Confirmation* c){
        //     std::cout << "Server " <<"("<<serverinfo->machineid()<<") just restarted and needs to be resynched with counterpart"<<std::endl;
        //     std::string backupServerType;

        //     // YOUR CODE HERE


        //     return Status::OK;
        // }
    };

    void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){

        // //setup coordinator stub
        // //std::cout<<"synchronizer stub"<<std::endl;
        std::string target_str = coordIP + ":" + coordPort;
        coordinator_stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
        // //std::cout<<"MADE STUB"<<std::endl;

        ServerInfo serverInfo;
        ID id;
        grpc::ClientContext context;

        clusterID = ((synchID -1) % 3) + 1;

        serverInfo.set_clusterid(clusterID);
        serverInfo.set_hostname("localhost");
        serverInfo.set_port(port_no);

        //send init heartbeat
        grpc::Status status = coordinator_stub->registerSynch(&context, serverInfo, &id);

        if (!status.ok() || id.id() == -1) {
            std::cerr << "Error Connecting to Coordinator. Restart to try again." << std::endl;
            exit(1);
        }

        machineID = id.id();

        std::cout << "Paired with cluster" << clusterID << " server" << machineID << std::endl;

        //set up as server
        //localhost = 127.0.0.1
        std::string server_address("127.0.0.1:"+port_no);

        SynchServiceImpl service;

        //grpc::EnableDefaultHealthCheckService(true);
        //grpc::reflection::InitProtoReflectionServerBuilderPlugin();

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<Server> server(builder.BuildAndStart());

        if (!server) {
            std::cerr << "Failed to start server on " << server_address << std::endl;
            exit(1);
        }
        
        std::cout << "Server listening on " << server_address << std::endl;


        //std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);


        // Wait for the server to shutdown. Note that some other thread must be
        server->Wait();
    }


    /*
        TODO List:
        -Implement service calls
        -Set up initial single heartbeat to coordinator
        -Set up thread to run synchronizer algorithm
        */
    int main(int argc, char** argv) {

    int opt = 0;
    std::string coordIP = "127.0.0.1";
    std::string coordPort = "9090";
    std::string port = "9001";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1){
        switch(opt) {
            case 'h':
                coordIP = optarg;
                break;
            case 'k':
                coordPort = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'i':
                synchID = std::stoi(optarg);
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
    }

    void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){
        // //setup coordinator stub
        // //std::cout<<"synchronizer stub"<<std::endl;
        std::string target_str = coordIP + ":" + coordPort;
        std::unique_ptr<CoordService::Stub> coord_stub;
        coord_stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
        // //std::cout<<"MADE STUB"<<std::endl;

        ServerInfo serverInfo;
        ID id;
        grpc::ClientContext context;

        clusterID = ((synchID -1) % 3) + 1;

        serverInfo.set_clusterid(clusterID);
        serverInfo.set_hostname("localhost");
        serverInfo.set_port(port);

        //send init heartbeat
        grpc::Status status = coord_stub->registerSynch(&context, serverInfo, &id);

        if (!status.ok()) {
            std::cerr << "Error Connecting to Coordinator. Restart to try again." << std::endl;
            exit(1);
        }

        machineID = id.id();

        std::cout << "Paired with cluster" << clusterID << " server" << machineID << std::endl;

        // //     //TODO: begin synchronization process
        // //     while(true){
        // //         //change this to 30 eventually
        // //         sleep(20);
        // //         //synch all users file 
        // //             //get list of all followers

        // //             // YOUR CODE HERE
        // //             //set up stub
        // //             //send each a GetAllUsers request
        // //             //aggregate users into a list
        // //             //sort list and remove duplicates

        // //             // YOUR CODE HERE

        // //             //for all the found users
        // //             //if user not managed by current synch
        // //             // ...

        // //             // YOUR CODE HERE

        // // 	    //force update managed users from newly synced users
        // //             //for all users
        // //             for(auto i : aggregated_users){
        // //                 //get currently managed users
        // //                 //if user IS managed by current synch
        // //                     //read their follower lists
        // //                     //for followed users that are not managed on cluster
        // //                     //read followed users cached timeline
        // //                     //check if posts are in the managed tl
        // //                     //add post to tl of managed user    
                
        // //                      // YOUR CODE HERE
        // //                     }
        // //                 //}
        // //             //}
        // //     }
        return;
    }

    std::vector<std::string> get_lines_from_file(std::string filename){
        std::vector<std::string> users;
        std::string user;
        std::ifstream file; 
        file.open(filename);
        if(file.peek() == std::ifstream::traits_type::eof()){
            //return empty vector if empty file
            //std::cout<<"returned empty vector bc empty file"<<std::endl;
            file.close();
            return users;
        }
        while(file){
            getline(file,user);

            if(!user.empty()) {
                    users.push_back(user);
            }
        } 

        file.close();

        //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
        /*for(int i = 0; i<users.size(); i++){
        std::cout<<users[i]<<std::endl;
        }*/ 

        return users;
    }

    bool file_contains_user(std::string filename, std::string user){
        std::vector<std::string> users;
        //check username is valid
        users = get_lines_from_file(filename);
        for(int i = 0; i<users.size(); i++){
            //std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
            if(user == users[i]){
            //std::cout<<"found"<<std::endl;
            return true;
            }
        }
        //std::cout<<"not found"<<std::endl;
        return false;
    }

    std::vector<std::string> get_all_users_func(int synchID){
        //read all_users file master and client for correct serverID
        std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
        std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
        //take longest list and package into AllUsers message
        std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
        std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

        if(master_user_list.size() >= slave_user_list.size())
            return master_user_list;
        else
            return slave_user_list;
        }

        std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl){
        std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
        std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
        if(tl){
            master_fn.append("_timeline");
            slave_fn.append("_timeline");
        }else{
            master_fn.append("_follow_list");
            slave_fn.append("_follow_list");
        }

        std::vector<std::string> m = get_lines_from_file(master_fn);
        std::vector<std::string> s = get_lines_from_file(slave_fn);

        if(m.size()>=s.size()){
            return m;
        }else{
            return s;
        }
    }



    //creates connection with coordinator
    //registers with coordinator

    //listens for service calls from server
    //upon those calls call the server and then multicast to other synchronizers

    //listens for service calls from other synchronizers
