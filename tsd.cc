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
#include <sys/stat.h>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <sstream>
#include <iostream>
#include <memory>
#include <filesystem>
#include <fcntl.h>
#include <semaphore.h>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdlib.h>
#include <unistd.h>
#include <thread>
#include <mutex>
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
  std::shared_ptr<ServerReaderWriter<Message, Message>> stream = nullptr;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client (and their data)
std::vector<Client*> client_db;
std::mutex db_mutex;

std::vector<Client*> client_list;
std::mutex list_mutex;


int clusterID;
int machineID;
bool master;

std::string synchPort = "null";

std::string slaveHostname = "null";
std::string slavePort = "null";

std::unique_ptr<SynchService::Stub> stub_synch;
std::unique_ptr<SNSService::Stub> stub_slave;

std::string semName;




class SNSServiceImpl final : public SNSService::Service {
  
  //replies with ListReply where all_users is the name of all users and followers is the list of users that follow the user
  //Note: request contains the user
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    
    /*********
    YOUR CODE HERE
    **********/
    db_mutex.lock();
    Client* c = getClient(request->username());
    db_mutex.unlock();

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
    for (Client* client : client_list) {
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
    Client* u1 = getClient_all(request->username());
    Client* u2 = getClient_all(request->arguments()[0]);

    if (u1 == nullptr || u2 == nullptr || u1 == u2) {
      reply->set_msg("I");
      return Status::OK;
    }


    //add u2 to u1 following
    u1->client_following.push_back(u2);

    std::string machineDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + "/";
    appendText(u2->username, machineDir + "u" + u1->username + "/following");

    //if u2 is in cluster
    if (containsUser(u2->username)) {
      u2->client_followers.push_back(u1);
      appendText(u1->username, machineDir + "u" + u2->username + "/followers");

      if (master && slaveHostname != "null") {
        ClientContext context2;
        Reply reply2;
        stub_slave->Follow(&context2, *request, &reply2);
      }
    } 
    //if u2 is outside of cluster (only do anything else if also master)
    else if (master) {
      //make call to slave for u1 following u2
      //make call to synchronizer for u1 following u2
      if (slaveHostname != "null") {
        ClientContext context2;
        Reply reply2;
        stub_slave->Follow(&context2, *request, &reply2);
      }
      if (synchPort != "null") {
        ClientContext context3;
        Users users;
        users.add_users(std::stoi(u1->username));
        users.add_users(std::stoi(u2->username));
        Confirmation confirmation;

        std::cerr<<"newFollowServ"<<std::endl;
        stub_synch->newFollowServ(&context3, users, &confirmation);
      }
    }

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
    db_mutex.lock();
    Client* u1 = getClient(request->username());
    Client* u2 = getClient(request->arguments()[0]);
    db_mutex.unlock();

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
    db_mutex.lock();
    Client* c = getClient(request->username());
    db_mutex.unlock();

    //if the client was not found (DNE) then create it and add it to the database
    if (c == nullptr) {
      std::cerr << "adding new user" << std::endl;
      c = new Client;
      c->username = request->username();
      db_mutex.lock();
      client_db.push_back(c);
      db_mutex.unlock();
      //create new files to store data about its timeline
      std::string userDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + "/u" + c->username;
      std::cerr << userDir << std::endl;
      std::filesystem::create_directories(userDir);
      std::ofstream postsFile(userDir + "/posts.txt");
      std::ofstream timelineFile(userDir + "/timeline.txt");
      std::ofstream followingFile(userDir + "/following.txt");
      std::ofstream followerFile(userDir + "/followers.txt");
      postsFile.close();
      postsFile.close();
      followingFile.close();
      followerFile.close();

      //std::cerr << "adding client to local files" <<std::endl;

      appendText(c->username, "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + "/clients");

      //resync client_list
      std::vector<std::string> newClients = get_lines_from_file_pvt("data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + "/clients.txt");
      //delete and clear client list
      for (Client* c : client_list) {
        delete c;
      }
      client_list.clear();

      //add new usernames to client_list
      for (std::string usernm: newClients) {
        Client* newC = new Client;
        newC->username = usernm;
        client_list.push_back(newC);
      }
      
      //std::cerr << "added client to local files" << std::endl;
      //if master make call to slave and synchronizer that user logged in
      if (master) {
        if (slaveHostname != "null") {
          ClientContext context2;
          Reply reply2;
          stub_slave->Login(&context2, *request, &reply2);
        }
        if (synchPort != "null") {
          ClientContext context3;
          ID id;
          id.set_id(std::stoi(c->username));
          Confirmation confirmation;

          std::cerr<<"newClientServ"<<std::endl;
          stub_synch->newClientServ(&context3, id, &confirmation);
        }
      }
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
	    ServerReaderWriter<Message, Message>* streem1) override {
    
    std::shared_ptr<ServerReaderWriter<Message, Message>> streem(streem1);

    
    /*********
    YOUR CODE HERE
    **********/
    Message init;
    streem->Read(&init);

    std::string username = init.username();
    db_mutex.lock();
    Client* c = getClient(username);
    db_mutex.unlock();
    std::string userDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + 
                  "/u" + username;

    //assign clients timeline so other clients can find it in database and stream to it
    c->stream = streem;

    //get the last 20 posts of users they follow and send them to client
    std::vector<Message> posts = getPosts(userDir+"/timeline", 20);
    for (Message message : posts) {
      streem->Write(message);
    }
    std::thread reader([this, streem, c, userDir, username]() {
      Message m;
      while(streem->Read(&m)) {
        std::cerr<<"read"<<std::endl;
        //get client and formatted string from message
        std::string ffo = formatFileOutput(m);

        //add the post to the user's list of posts
        appendPost(ffo, userDir+"/posts");

        //for each of their followers, attempt to write to their stream channel if they are in timeline mode and add the post to their file for their timeline
        for (Client* follower : c->client_followers) {
          if (containsUser(follower->username)) {
            if (master && slaveHostname != "null") {
              //service call Post to slave
              ClientContext context2;
              Reply reply2;
              stub_slave->PostServ(&context2, m, &reply2);
            }
            std::string followerDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + 
                      "/u" + follower->username;
            appendPost(ffo, followerDir+"/timeline");
          } else if (master) {
            std::cerr<<"master"<<std::endl;
            if (synchPort != "null") {
              std::cerr<<"newPostServ1"<<std::endl;
              ClientContext context3;
              Post post3;
              post3.set_username(username);
              post3.set_post(m.msg());
              google::protobuf::Timestamp* timestamp_p = new google::protobuf::Timestamp(m.timestamp());
              post3.set_allocated_timestamp(timestamp_p);
              Confirmation confirmation3;

              std::cerr<<"newPostServ2"<<std::endl;
              grpc::Status status = stub_synch->newPostServ(&context3, post3, &confirmation3);
              if (!status.ok()) {
                std::cerr<<"Error posting"<<std::endl;
              }
              std::cerr<<"newPostServ3"<<std::endl;

            }
            if (slaveHostname != "null") {
              //service call Post to slave
              ClientContext context2;
              Reply reply2;
              stub_slave->PostServ(&context2, m, &reply2);
            }
          }
        }
      }
      std::cerr<<"ERROR, SHOULD NOT SEE"<<std::endl;
    });

    std::thread writer([this, streem, userDir, username](){
      while(1) {
        sleep(20);

        std::cerr<<"checking timeline..."<<std::endl;

        //check if timeline has been update, if so, read last 20 posts and write them to streem
        namespace fs = std::filesystem;

        struct stat timelineFileInfo;
        if(stat((userDir+"/timeline.txt").c_str(), &timelineFileInfo) == 0) {
          auto now = std::time(nullptr);
          auto lastModified = timelineFileInfo.st_mtime;

          if (now - lastModified < 20) {
            std::cerr<<"refreshing timeline of user " << username << std::endl;
            //get the last 20 posts of users they follow and send them to client
            std::vector<Message> posts = getPosts(userDir+"/timeline", 20);
            for (Message message : posts) {
              streem->Write(message);
            }
          }
        }

      }
    });

    reader.join();
    writer.join();

    return Status::OK;
  }

  /*ToDo:
  -passed message that u1 posts
  -update u1's posts file
  -update all of u1's followers (that are local) timelines
  */
  Status PostServ(ServerContext* context, const Message* message,  Reply* reply) {
    std::string username = message->username();
    db_mutex.lock();
    Client* c = getClient(username);
    db_mutex.unlock();
    std::string userDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + 
                    "/u" + username;
    std::string ffo = formatFileOutput(*message);

    appendPost(ffo, userDir+"/posts");
    for (Client* follower : c->client_followers) {
      std::string followerDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID) + 
                                  "/u" + follower->username;
      appendPost(ffo, followerDir+"/timeline");
    }

    return Status::OK;
  }

  private :

    void timelineWriter(std::shared_ptr<ServerReaderWriter<Message, Message>> streem, std::string machineDir, std::string username) {
      while(1) {
        sleep(20);

        //check if timeline has been update, if so, read last 20 posts and write them to streem
        namespace fs = std::filesystem;

        struct stat timelineFileInfo;
        if(stat((machineDir+"/u"+username+"/timeline.txt").c_str(), &timelineFileInfo) == 0) {
          auto now = std::time(nullptr);
          auto lastModified = timelineFileInfo.st_mtime;

          if (now - lastModified < 20) {
            std::cerr<<"refreshing timeline of user " << username << std::endl;
            //get the last 20 posts of users they follow and send them to client
            std::vector<Message> posts = getPosts(machineDir+"/u"+username+"/timeline", 20);
            for (Message message : posts) {
              streem->Write(message);
            }
          }
        }

      }
    }

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

    Client* getClient_all(std::string username) {
      db_mutex.lock();
      for (Client* c1 : client_db) {
        if (c1->username == username) {
          db_mutex.unlock();
          return c1;
        }
      }
      db_mutex.unlock();
      list_mutex.lock();
      for (Client* c2 : client_list) {
        if (c2->username == username) {
          list_mutex.unlock();
          return c2;
        }
      }
      return nullptr;
      list_mutex.unlock();
    }

    //adds a post to the file by reading the file into memory and then re-writing it with the post at the top
    int appendPost(std::string ffo, std::string filename) {

      std::cerr << "writing to " << filename << std::endl;

      sem_t *sem = sem_open(semName.c_str(), O_CREAT , 0644, 1);
      if (sem == SEM_FAILED) {
          std::cerr << "Failed to open semaphore" << std::endl;
          return 1;
      }

      // Wait for the semaphore
      if (sem_wait(sem) == -1) {
          std::cerr << "Failed to wait for semaphore" << std::endl;
          sem_close(sem);
          return 1;
      }

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

      // Post the semaphore
      if (sem_post(sem) == -1) {
          std::cerr << "Failed to post semaphore" << std::endl;
      }

      // Close the semaphore
      sem_close(sem);

      std::cerr << "Finished writing to " << filename << std::endl;


      return 0;
    }

    int appendText(std::string text, std::string filename) {

      std::string file = filename + ".txt";

      sem_t *sem = sem_open(semName.c_str(), O_CREAT , 0644, 1);
      if (sem == SEM_FAILED) {
          std::cerr << "Failed to open semaphore" << std::endl;
          return -1;
      }

      // Wait for the semaphore
      if (sem_wait(sem) == -1) {
          std::cerr << "Failed to wait for semaphore" << std::endl;
          sem_close(sem);
          return -1;
      }
      
      // Open the file in append mode
      std::ofstream outFile(file, std::ios::app);
      if (outFile.is_open()) {
          outFile << text <<std::endl;
          outFile.close();
      } else {
          std::cerr << "Unable to open file: " << filename << std::endl;
      }

      // Post the semaphore
      if (sem_post(sem) == -1) {
          std::cerr << "Failed to post semaphore" << std::endl;
      }

      // Close the semaphore
      sem_close(sem);


      return 1;
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

    std::vector<std::string> get_lines_from_file_pvt(std::string filename){
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


    bool containsUser(std::string username) {
      for (auto c : client_db) {
        if (c->username == username) {
          return true;
        }
      }
      return false;
    }

    Client* findUser(std::string username) {
      for (Client* c : client_db) {
        if (c->username == username) {
          return c;
        }
      }
      return nullptr;
    }


};



Client* getClient_all2(std::string username) {
      db_mutex.lock();
      for (Client* c1 : client_db) {
        if (c1->username == username) {
          db_mutex.unlock();
          return c1;
        }
      }
      db_mutex.unlock();
      list_mutex.lock();
      for (Client* c2 : client_list) {
        if (c2->username == username) {
          list_mutex.unlock();
          return c2;
        }
      }
      list_mutex.unlock();
      return nullptr;
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

        std::string synch_address = "0.0.0.0:"+synchPort;
        auto synchChan = grpc::CreateChannel(synch_address, grpc::InsecureChannelCredentials());
        stub_synch = std::make_unique<SynchService::Stub>(synchChan);
      }
      if (slaveHostname == "null" && response.slavehostname() != "null") {
        slaveHostname = response.slavehostname();
        slavePort = response.slaveport();

        std::string slave_address = slaveHostname+":"+slavePort;
        auto slaveChan = grpc::CreateChannel(slave_address, grpc::InsecureChannelCredentials());
        stub_slave = std::make_unique<SNSService::Stub>(slaveChan);
      }
      std::cerr << "master" << master << " synchPort" << synchPort << " slaveIP" << slaveHostname << ":" <<slavePort << std::endl;


      sleep(5);
    }
  });

  //ToDo: CREATE THREAD THAT SYNCHS MEMORY EVERY 20 SECONDS
    //for each user
      //if any of their files have been updateed in last 20 seconds
        //read that file into memory (following, followers, timeline)
  std::thread synchronize([]() {
    std::string machineDir = "data/cluster" + std::to_string(clusterID) + "/machine" + std::to_string(machineID);
    namespace fs = std::filesystem;
    std::vector<std::string> newClients;
    
    while(1) {
      std::cerr<<"starting resync process"<<std::endl;
      //synch clients and followers
      struct stat clientFileInfo;
      if (stat((machineDir + "/clients.txt").c_str(), &clientFileInfo) == 0) {
        auto now = std::time(nullptr);
        auto clientsLastModified = clientFileInfo.st_mtime;
        
        // Check if the file was modified in the last 20 second
        if (now - clientsLastModified < 15) {
          std::cerr<<"synching clients..."<<std::endl;
          newClients = get_lines_from_file(machineDir+"/clients.txt");
          //delete and clear client list
          for (Client* c : client_list) {
            delete c;
          }
          client_list.clear();

          //add new usernames to client_list
          for (std::string usernm: newClients) {
            Client* newC = new Client;
            newC->username = usernm;
            client_list.push_back(newC);
          }
        }
      }

      //synch followers
      db_mutex.lock();
      for (Client* c : client_db) {
        struct stat tempFileInfo;
        if (stat((machineDir + "/u"+c->username+"/followers.txt").c_str(), &tempFileInfo) == 0) {
            auto now = std::time(nullptr);
            auto followersLastModified = tempFileInfo.st_mtime;

            if (now - followersLastModified < 15) {
              std::cerr<<"synching followers of " << c->username <<std::endl;
              std::vector<std::string> newFollowers = get_lines_from_file(machineDir + "/u"+c->username+"/followers.txt");

              c->client_followers.clear();

              for (std::string newFollowerName : newFollowers) {
                db_mutex.unlock();
                Client* newFollower = getClient_all2(newFollowerName);
                db_mutex.lock();
                if (newFollower != nullptr) {
                  c->client_followers.push_back(newFollower);
                }
              }
            }
        }
      }
      db_mutex.unlock();
      

      sleep(15);
    }

  });

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
  clusterID = clusterId;
  machineID = serverId;
  semName = "/c"+std::to_string(clusterID)+"m"+std::to_string(machineID);
  sem_t *sem = sem_open(semName.c_str(), O_CREAT , 0644, 1);
  sem_init(sem, 1, 1);
  RunServer(clusterId, serverId, coordIP, coordPort, port);

  return 0;
}
