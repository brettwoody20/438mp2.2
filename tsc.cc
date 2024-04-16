#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <sstream>
#include <unistd.h>
#include <csignal>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::SNSService;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::ID;


void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  //TODO: Change args
  Client(const std::string& cname,
	 const int& uname,
	 const std::string& p)
    :coordinatorIP(cname), userID(uname), coordinatorPort(p) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  int userID;
  std::string coordinatorIP;
  std::string coordinatorPort;

  //std::string serverIP;
  //std::string serverPort;
  
  // client stub for server and coordinator communication
  std::unique_ptr<SNSService::Stub> stub_snss;
  std::unique_ptr<CoordService::Stub> stub_coord;
  
  IServerInfo GetServer();
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
  std::vector<std::string> split(const std::string& str);
  void toUpperCase(std::string& str) const;
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  //ToDO: implement new connection to coordinator, use stub to call GetServerID

  //create new channel and stub FOR COORDINATOR
  //std::cout << "Attempting to open channel with coordinator..." << std::endl;
  std::string login_info_coord = coordinatorIP + ":" + coordinatorPort;
  auto coordChan = grpc::CreateChannel(login_info_coord, grpc::InsecureChannelCredentials()); 
  stub_coord = std::make_unique<CoordService::Stub>(coordChan);
  
  //attempt to get server info from coordinator
  IServerInfo server_info = GetServer();
  if (!server_info.grpc_status.ok() || server_info.comm_status == IStatus::FAILURE_UNKNOWN) {
    return -1;
  }

  //create new channel and stub FOR SERVER
  //std::cout << "Attempting to open channel with server..." << std::endl;
  std::string login_info = server_info.hostname + ":" + server_info.port;
  auto serverChan = grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()); 
  stub_snss = std::make_unique<SNSService::Stub>(serverChan);
  //attempt to login, return result
  IReply reply = Login();
  if (reply.grpc_status.ok()) {
    if (reply.comm_status == IStatus::SUCCESS) {
      return 1;
    }
  }
    return -1;
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /*********
  YOUR CODE HERE
  **********/
  
  //initialize IReply to be constructed and returned
  IReply ire;
  
  //vector to hold arguments
  std::vector<std::string> cmds = split(input);
  toUpperCase(cmds[0]);

  if (cmds[0] == "FOLLOW") {
    ire = Follow(cmds[1]);
  } else if (cmds[0] == "UNFOLLOW") {
    ire = UnFollow(cmds[1]);
  } else if (cmds[0] == "LIST") {
    ire = List();
  } else if (cmds[0] == "TIMELINE") {
    //Timeline is called in client run function when these conditions are met, so just ensure they are
    ire.grpc_status = grpc::Status::OK;
    ire.comm_status = IStatus::SUCCESS;
  }


  return ire;
}


void Client::processTimeline()
{
    Timeline(std::to_string(userID));
}



// List Command
IReply Client::List() {

    IReply ire;

    /*********
    YOUR CODE HERE
    **********/

    //prepare a service call
    ClientContext context;
    Request request;
    request.set_username(std::to_string(userID));
    ListReply listReply;

    grpc::Status status = stub_snss->List(&context, request, &listReply);

    ire.grpc_status = status;

    //check ire status and ensure that the server did not encounter runtime error (this is communicated by a failure message being
    //  passed into the first index of followers)
    if (ire.grpc_status.ok()) {

      //check that the server did not encounter runtime error (this is communicated by a failure message being
      //  passed into the first index of followers), if there was, return that
      if (listReply.followers_size() > 0 && listReply.followers(0) == "FAILURE UNKNOWN") {
        ire.comm_status = IStatus::FAILURE_UNKNOWN;
        return ire;
      }

      //copy all of the users and followers returned into IReply object
      for (int i = 0; i < listReply.all_users_size(); ++i) {
        ire.all_users.push_back(listReply.all_users(i));
      }
      for (int i = 0; i < listReply.followers_size(); ++i) {
        ire.followers.push_back(listReply.followers(i));
      }
      ire.comm_status = IStatus::SUCCESS;

    } else {
      ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
      
    /***
    YOUR CODE HERE
    ***/

    //prepare service call
    ClientContext context;
    Request request;
    request.set_username(std::to_string(userID));
    request.add_arguments(username2);
    Reply reply;

    grpc::Status status = stub_snss->Follow(&context, request, &reply);

    ire.grpc_status = status;
    //check status
    if (ire.grpc_status.ok()) {
      switch (reply.msg()[0]) {
        case 'S':
          ire.comm_status = IStatus::SUCCESS;
          break;
        case 'I':
          ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
          break;
        default:
          ire.comm_status = IStatus::FAILURE_UNKNOWN;
          break;
      }
      
    } else {
      ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }

    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    /***
    YOUR CODE HERE
    ***/

    //construct service call
    ClientContext context;
    Request request;
    request.set_username(std::to_string(userID));
    request.add_arguments(username2);
    Reply reply;

    grpc::Status status = stub_snss->UnFollow(&context, request, &reply);
    ire.grpc_status = status;
    if (ire.grpc_status.ok()) {

      //unfollow can encounter various runtime errors that are communicated by a single char in reply
      switch (reply.msg()[0]) {
        case 'S':
          ire.comm_status = IStatus::SUCCESS;
          break;
        case 'I':
          ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
          break;
        case 'U':
          ire.comm_status = IStatus::FAILURE_NOT_A_FOLLOWER;
          break;
        default:
          ire.comm_status = IStatus::FAILURE_UNKNOWN;
          break;
      }

    } else {
      ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }
    
    return ire;
}

// Login Command  
IReply Client::Login() {

  IReply ire;

  /***
    YOUR CODE HERE
  ***/
  //construct service call
  ClientContext context;
  Request request;
  request.set_username(std::to_string(userID));
  Reply reply;

  grpc::Status status = stub_snss->Login(&context, request, &reply);
  ire.grpc_status = status;
  if (ire.grpc_status.ok()) {
    //follow can encounter various runtime errors that are communicated by a single char in reply
    switch(reply.msg()[0]) {
      case 'S':
        ire.comm_status = IStatus::SUCCESS;
        break;
      case 'F':
        ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
        break;
      default:
        ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }
  } else {
    ire.comm_status = IStatus::FAILURE_UNKNOWN;
  }

  return ire;
}

IServerInfo Client::GetServer() {

  //std::cout << "Attempting to retrive server from coordinator..." << std::endl;

  IServerInfo serve;

  //construct service call
  ClientContext context;
  ID id;
  id.set_id(userID);
  ServerInfo serverInfo;

  //call service call
  grpc::Status status = stub_coord->GetServer(&context, id, &serverInfo);

  //evaluate response and package it into serve
  serve.grpc_status = status;
  serve.comm_status = IStatus::FAILURE_UNKNOWN; //this is changed as long as server was successfully found
  if(serve.grpc_status.ok()) {
    if (serverInfo.serverid() != -1) {
      serve.comm_status = IStatus::SUCCESS;
      serve.serverID = serverInfo.serverid();
      serve.hostname = serverInfo.hostname();
      serve.port = serverInfo.port();
      serve.type = IType::SERVER;
    }
  }

  //std::cout << "Retrieved server " << serve.hostname << ":" << serve.port << std::endl;

  return serve;
}

// Timeline Command
void Client::Timeline(const std::string& username) {


  
  /***
  YOUR CODE HERE
  ***/

  ClientContext context;

  //initialize a bidirectional wire with server timeline function that sends and recieves Message objects
  std::shared_ptr<ClientReaderWriter<Message, Message> > streem(stub_snss->Timeline(&context));

  //create a thread to parse user input and send any posts to server
  std::thread writer([streem, username]() {

    //The server requires a first message to properly initialize the client's timeline- a dummy message (this message is discarded after)
    Message dummy = MakeMessage(username,"dummy");
    streem->Write(dummy);

    //enter loop to get and send posts
    while(1) {
      std::string message = getPostMessage();
      Message mw = MakeMessage(username, message);
      streem->Write(mw);
    }

  });

  //create a reader thread to recieve Messages from the server
  std::thread reader([streem](){
    
    Message mr;
    while(streem->Read(&mr)) {
      std::time_t time = mr.timestamp().seconds();
      displayPostMessage(mr.username(), mr.msg(),time);
    }

  });
  
  reader.join();
  writer.join();
}

std::vector<std::string> Client::split(const std::string& str) {
  std::vector<std::string> ret;
  std::istringstream iss(str);
  std::string word;

  //push the next two words into return vector
  iss >> word;
  ret.push_back(word);
  iss >> word;
  ret.push_back(word);

  return ret;
}

void Client::toUpperCase(std::string& str) const
{
  std::locale loc;
  for (std::string::size_type i = 0; i < str.size(); i++)
    str[i] = toupper(str[i], loc);
}




//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string coordIP = "localhost";
  int username = 1;
  std::string coordPort = "9090";
    
  //TODO:
  // change args
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1){
    switch(opt) {
    case 'h':
      coordIP = optarg;break;
    case 'u':
      username = atoi(optarg);break;
    case 'k':
      coordPort = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(coordIP, username, coordPort);
  
  myc.run();
  
  return 0;
}
