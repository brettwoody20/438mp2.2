/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <sstream>
#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdlib.h>
#include <unistd.h>
#include <thread>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"

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
using grpc::Channel;
using grpc::ClientContext;
using csce438::SNSService;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::CoordService;
using csce438::HBResponse;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::SynchService;
using csce438::ID;
using csce438::Users;
using csce438::Post;


struct Client {
  std::string username;
  bool connected = false;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = nullptr;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client (and their data)
std::vector<Client*> client_db;

bool master;

std::string synchPort = "null";

std::string slaveHostname = "null";
std::string slavePort = "null";

std::unique_ptr<SynchService::Stub> stub_synch;
std::unique_ptr<SNSService::Stub> stub_slave;




class SNSServiceImpl final : public SNSService::Service {
  
  //replies with ListReply where all_users is the name of all users and followers is the list of users that follow the user
  //Note: request contains the user
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    
    /*********
    YOUR CODE HERE
    **********/

    Client* c = getClient(request->username());

    //if client is for some reason not found, return error message in the reply (followers vector chosen arbitrarily)
    if (c == nullptr) {
      list_reply->add_followers("FAILURE UNKNOWN");
      return Status::OK;
    }

    //add all followers of user to reply
    for (Client* follower : c->client_followers) {
      list_reply->add_followers(follower->username);
    }

    //add all clients' usernames to reply
    for (Client* client : client_db) {
      list_reply->add_all_users(client->username);
    }

    return Status::OK;
  }

  //Inputs: request (usernmae, arguments)
  //Func: verify the user exists and then add it to the appropriate lists of following/followers
  //Outputs: replies with the success of the command. 
  //Reply:  S : succes, I : Invalid users.

  /*ToDo:
  -If master, make call to slave
  -passed u1 wants to follow u2
  -add u2 into u1's following and replicate that in local files
  -if u2 is on cluster (can use mathematical equation or search users) then add it to u2's files
    otherwise make new follow rpc to synch
  */
  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
    
    //first validate that both users exist and are seperate users
    Client* u1 = getClient(request->username());
    Client* u2 = getClient(request->arguments()[0]);

    if (u1 == nullptr || u2 == nullptr || u1 == u2) {
      reply->set_msg("I");
      return Status::OK;
    }


    //if both users exist and are different, then add them to appropriate following/followers list (u1 follows u2)
    u1->client_following.push_back(u2);
    u2->client_followers.push_back(u1);

    reply->set_msg("S");
    
    return Status::OK; 
  }

  //Inputs: request (username, arguements)
  //Func: verify the users exists, remove them from appropriate lists of following/followers
  //Outputs: replies with success of the command.
  //Reply: S : succes, I : invalid user, U : doesn't follow

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/

    Client* u1 = getClient(request->username());
    Client* u2 = getClient(request->arguments()[0]);

    //ensure both usernames exist in database as well as that they are not the same
    if (u1 == nullptr || u2 == nullptr || u1 == u2) {
      reply->set_msg("I");
      return Status::OK;
    }

    //###if everything is valid, u1 unfollows u2###

    //find u2 in u1's following list
    auto it1 = std::find(u1->client_following.begin(), u1->client_following.end(), u2);
    
    //if it wasn't found, return the error, otherwise remove it from the vector
    if (it1 == u1->client_following.end()) {
      reply->set_msg("U");
      return Status::OK;
    } else {
      u1->client_following.erase(it1);
    }


    //attempt to find and remove u1 from u2 followers list (verification isn't necesarry as the end results of finding and removing are both it not being in the list, the error would be with following)
    auto it2 = std::find(u2->client_followers.begin(), u2->client_followers.end(), u1);
    if ( it2 != u2->client_followers.end()) {
      u2->client_followers.erase(it2);
    }

    reply->set_msg("S");
    return Status::OK;
  }

  //Inputs: request (username)
  //Func: Initializes user in system if they don't exist, logs them in if they aren't logged in already.
  //Outputs: replies with success of the command.
  //Reply: S : succes, F : failure (logged in already)

  /*ToDo:
  -upon login, create directory for user
  */
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    
    /*********
    YOUR CODE HERE
    **********/

    Client* c = getClient(request->username());

    //if the client was not found (DNE) then create it and add it to the database
    if (c == nullptr) {
      c = new Client;
      c->username = request->username();
      client_db.push_back(c);
      //create new files to store data about its timeline
      std::ofstream postsFile(c->username+"_posts.txt");
      std::ofstream timelineFile(c->username+"_timeline.txt");
      postsFile.close();
      postsFile.close();

    }

    //NOTE: FOR 2.2 REQUIREMENTS, THE LOGIC THAT STOPPED TWO USERS FROM SIGNING IN WHEN CONNECTED HAS BEED REMOVED
    //IN ORDER TO RESTORE THIS, FOLLOW COMMENTS. IN ORDER TO PROPERLY INSTALL  THE LOGIC, CLIENTS MUST SIGNAL A LOGOUT INSTEAD OF CTRL+C.

    //If client is alread logged in then return they can't log in here, otherwise log them in
    //The F and S are used to communicate with the client which branch occured
    if (c->connected) {
      reply->set_msg("S"); //RESTORE TO "F"
    } else {
      reply->set_msg("S");
      c->connected = true;
    }
    
    return Status::OK;
  }

  //ToDo: 
  //-write and read from correct files
  //-server checks if file was updated every 30 seconds instead of directly streaming posts
  //-if client posts and is master, use Post rpc to slave
  //-if client posts, synchronize local user files if they are affected, then use newPostServ rpc to synch
  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* streem) override {
    


    /*********
    YOUR CODE HERE
    **********/

    Message m;
    while(streem->Read(&m)) {

      //get client and formatted string from message
      std::string username = m.username();
      Client* c = getClient(username);
      std::string ffo = formatFileOutput(m);

      //if the clients ServerReaderWriter stream member has not been used initialized (first message) set up timeline
      //  Note: the first message is a dummy message with no actual post, discard it after
      if (c->stream == nullptr) {
        //assign clients timeline so other clients can find it in database and stream to it
        c->stream = streem;

        //get the last 20 posts of users they follow and send them to client
        std::vector<Message> posts = getPosts(username+"_timeline", 20);
        for (Message message : posts) {
          streem->Write(message);
        }

      } else { //any message other than the first

        //add the post to the user's list of posts
        appendPost(ffo, username+"_posts");

        //for each of their followers, attempt to write to their stream channel if they are in timeline mode and add the post to their file for their timeline
        for (Client* follower : c->client_followers) {
          if (follower->stream != nullptr) {
            follower->stream->Write(m);
          }
          appendPost(ffo, follower->username+"_timeline");
        }

      }
    }
    
    return Status::OK;
  }

  /*ToDo:
  -passed message that u1 posts
  -update u1's posts file
  -update all of u1's followers (that are local) timelines
  */
  Status Post(ServerContext* context, const Message* message,  Reply* reply) {

    return Status::OK;
  }

  private :

    //returns a pointer to a client in the database given their usernam
    //  ret is nullptr if user is not found
    Client* getClient(std::string username) {
      Client* ret = nullptr;
      for (Client* c : client_db) {
        if (c->username == username) {
          ret = c;
        }
      }
      return ret;
    }

    //adds a post to the file by reading the file into memory and then re-writing it with the post at the top
    int appendPost(std::string ffo, std::string filename) {
      // Open the file for reading
      std::ifstream inputFile(filename+".txt");
      if (!inputFile.is_open()) {
          return 1;
      }

      // Read the contents of the file into a string
      std::stringstream buffer;
      buffer << inputFile.rdbuf();
      std::string fileContents = buffer.str();

      inputFile.close();

      // Open the file for writing (truncated)
      std::ofstream outputFile(filename+".txt");
      if (!outputFile.is_open()) {
          return 1;
      }

      // Write the new data at the beginning of the file
      outputFile << ffo << fileContents;

      // Close the output file
      outputFile.close();

      return 0;
    }

    //formats a string og the Message
    std::string formatFileOutput(const Message& m) {
      int64_t seconds = m.timestamp().seconds();
      std::string ret = "T " + std::to_string(seconds) + 
                  "\nU http://twitter.com/" + m.username() + 
                  "\nW " + m.msg() + "\n";
      return ret;
    }

    //gets the last <numPosts> message from <filename> and returns them in a vector
    std::vector<Message> getPosts(std::string filename, int numPosts) {
      //std::cout << "getting posts from " << filename << ".txt" << std::endl;
      std::vector<Message> ret;

      std::ifstream inputFile(filename+".txt");
      if (!inputFile.is_open()) {
          return ret;
      }

      std::string time, username, message;
      std::string line;
      int i = 0;
      //gets lines of file until the number of message read exceeds numPosts or the file ends.
      //reads the first part of the line to determine the data type and then constructs a message from it
      while (std::getline(inputFile, line)) {
          if (line.empty()) {

              // create new message
              Message m;
              m.set_username(username);
              m.set_msg(message);

              //set timestamp in message
              long long temp1 = strtoll(time.c_str(), NULL, 0);
              int64_t temp2 = temp1;
              google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
              timestamp->set_seconds(temp2);
              timestamp->set_nanos(0);
              m.set_allocated_timestamp(timestamp);

              ret.push_back(m);

              // Reset variables for the next entry
              time.clear();
              username.clear();
              message.clear();

              ++i;
              if (i >= numPosts) {
                break;
              }
          } else if (line[0] == 'T') {
              time = line.substr(2); // Extract timestamp (remove "T ")
          } else if (line[0] == 'U') {
              username = line.substr(21); // Extract username (remove "U http://twitter.com/")
          } else if (line[0] == 'W') {
              message = line.substr(2); // Extract message (remove "W ")
          }
      }
      inputFile.close();
      
      return ret;
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

        if(!user.empty())
          users.push_back(user);
      } 

      file.close();

      //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
      /*for(int i = 0; i<users.size(); i++){
        std::cout<<users[i]<<std::endl;
      }*/ 

      return users;
    }


};


void RunServer(int clusterId, int serverId, std::string coordIP,
                std::string coordPort, std::string port_no) {
          
  //create new channel and stub for coordinator
  std::cout << "Attempting to open channel with coordinator..." << std::endl;
  std::string coord_address = coordIP+":"+coordPort;
  auto coordChan = grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<CoordService::Stub> stub_coord = std::make_unique<CoordService::Stub>(coordChan);

  //create thread to continually send heartbeats to coordinator

  //create server info object to be repeatedly sent to coordinator
  ServerInfo serverInfo;
  serverInfo.set_clusterid(clusterId);
  serverInfo.set_machineid(serverId);
  serverInfo.set_hostname("0.0.0.0");
  serverInfo.set_port(port_no);
  
  //pass the stub and serverInfo to another thread where it will be continually sent to the coordinator
  std::thread heartBeat([&stub_coord, serverInfo]() {
    HBResponse response;
    while(1) {
      //std::cerr << "Sending heartbeat..." << std::endl;
      ClientContext context;
      grpc::Status status = stub_coord->Heartbeat(&context, serverInfo, &response);
      //#####ToDo: alter master status, slave/synchID based on response
      master = response.master();
      if (synchPort == "null" && response.synchport() != "null") {
        synchPort = response.synchport();
      }
      if (slaveHostname == "null" && response.slavehostname() != "null") {
        slaveHostname = response.slavehostname();
        slavePort = response.slaveport();
      }
      std::cerr << "Now... master:" << master << " synchPort: " << synchPort << " slaveIP: " << slaveHostname << ":" <<slavePort << std::endl;


      sleep(5);
    }
  });

  //ToDo: CREATE THREAD THAT SYNCHS MEMORY EVERY 20 SECONDS
    //for each user
      //if any of their files have been updateed in last 20 seconds
        //read that file into memory (following, followers, timeline)

  //CONSTRUCT SERVER ADDRESS FROM NEW ARGUMENTS
  //construct server address
  std::string server_address = "0.0.0.0:"+port_no;
  //create instance of SNSSServiceImpl to implement grpc methods
  SNSServiceImpl service;

  //serverBuilder is a grpc class provided to help construct servers
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  //wait for rpc requests
  server->Wait();
}

int main(int argc, char** argv) {
  /*arguments
  c: cluster id - def: 1
  s: server id - def: 1
  h: coordinator ip - def: "localhost"
  k: coordinator port - def: 9090
  p: port number
  */ 

  int clusterId = 1;
  int serverId = 1;
  std::string coordIP = "localhost";
  std::string coordPort = "9090";
  std::string port = "10000";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      case 'c':
        clusterId = atoi(optarg);break;
      case 's':
        serverId = atoi(optarg);break;
      case 'h':
        coordIP = optarg;break;
      case 'k':
        coordPort = optarg;break;
      case 'p':
        port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(clusterId, serverId, coordIP, coordPort, port);

  return 0;
}
