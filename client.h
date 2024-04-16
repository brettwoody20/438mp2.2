#include <iostream>
#include <string>
#include <ctime>
#include <vector>
#include <grpc++/grpc++.h>

#define MAX_DATA 256

enum IStatus
{
    SUCCESS,
    FAILURE_ALREADY_EXISTS,
    FAILURE_NOT_EXISTS,
    FAILURE_INVALID_USERNAME,
    FAILURE_NOT_A_FOLLOWER,
    FAILURE_INVALID,
    FAILURE_UNKNOWN
};

enum IType
{
  SERVER,
  FAILED
};

/*
 * IReply structure is designed to be used for displaying the
 * result of the command that has been sent to the server.
 * For example, in the "processCommand" function, you should
 * declare a variable of IReply structure and set based on
 * the type of command and the result.
 *
 * - FOLLOW/UNFOLLOW/TIMELINE command:
 * IReply ireply;
 * ireply.grpc_status = return value of a service method
 * ireply.comm_status = one of values in IStatus enum
 *
 * - LIST command:
 * IReply ireply;
 * ireply.grpc_status = return value of a service method
 * ireply.comm_status = one of values in IStatus enum
 * reply.users = list of all users who connected to the server at least onece
 * reply.followers = list of users who following current user;
 *
 * This structure is not for communicating between server and client.
 * You need to design your own rules for the communication.
 * 
 * stores grpc response status, some other status, a list of all users that connected to the server, and list of all followers
 */
struct IReply
{
    grpc::Status grpc_status;
    enum IStatus comm_status;
    std::vector<std::string> all_users;
    std::vector<std::string> followers;
};

struct IServerInfo
{
  grpc::Status grpc_status;
  enum IStatus comm_status;
  int serverID;
  std::string hostname; //IP address
  std::string port;
  enum IType type;
};

//some sort of way to parse stdin and wait for response, then returns that response as a string
std::string getPostMessage();
//takes in the sender, message, and time then outputs them in nice format
void displayPostMessage(const std::string& sender, const std::string& message, std::time_t& time);
  
class IClient
{
public:
  //main loop for client, connect to server, run main loop
  //  -calls processCommand and process timeline
  void run();
  
protected:
  /*
   * Pure virtual functions to be implemented by students
   */
  virtual int connectTo() = 0;
  virtual IReply processCommand(std::string& cmd) = 0;
  virtual void processTimeline() = 0;
  
private:
  //lists all of the command options
  void displayTitle() const;
  //waits for and parses line for valid input
  std::string getCommand() const;
  //displays the reply of the server
  //  -Inputs: 
  void displayCommandReply(const std::string& comm, const IReply& reply) const;
  void toUpperCase(std::string& str) const;
};


