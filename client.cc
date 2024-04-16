#include "client.h"

//main loop for client, attempts to connect to the server, lists possible commands then infinitely
//  enters loop to respond to input
void IClient::run()
{
  bool connected = false;
  int ret = connectTo();
  if (ret < 0) {
    std::cout << "connection failed: " << ret << std::endl;
    exit(1);
  }
  connected = true;
  displayTitle();
  while (1) {
    std::string cmd = getCommand();
    if (!connected) {
      if (connectTo() < 0) {
        std::cerr << "Command Failed" << std::endl;
        continue;
      } else {
        connected = true;
      }
    }
    IReply reply = processCommand(cmd);
    if (!reply.grpc_status.ok()) {
      std::cerr << "Command Failed" << std::endl;
      connected = false;
    } else {
      displayCommandReply(cmd, reply);
    }
    if (reply.grpc_status.ok() && reply.comm_status == SUCCESS
	&& cmd == "TIMELINE") {
      std::cout << "Now you are in the timeline" << std::endl;
      processTimeline();
    }
  }
}

//lists all the possible options in
void IClient::displayTitle() const
{
  std::cout << "\n========= TINY SNS CLIENT =========\n";
  std::cout << " Command Lists and Format:\n";
  std::cout << " FOLLOW <username>\n";
  std::cout << " UNFOLLOW <username>\n";
  std::cout << " LIST\n";
  std::cout << " TIMELINE\n";
  std::cout << "=====================================\n";
}

//pareses line for valid input
std::string IClient::getCommand() const
{
  std::string input;
  while (1) {
    std::cout << "Cmd> ";
    std::getline(std::cin, input);
    std::size_t index = input.find_first_of(" ");
    if (index != std::string::npos) {
      std::string cmd = input.substr(0, index);
      toUpperCase(cmd);
      if(input.length() == index+1){
	std::cout << "Invalid Input -- No Arguments Given\n";
	continue;
      }
      std::string argument = input.substr(index+1, (input.length()-index));
      input = cmd + " " + argument;
    } else {
      toUpperCase(input);
      if (input != "LIST" && input != "TIMELINE") {
	std::cout << "Invalid Command\n";
	continue;
      }
    }
    break;
  }
  return input;
}

// Displays the result of the reply ie the lists returned or the error messages returned
void IClient::displayCommandReply(const std::string& comm, const IReply& reply) const
{
  if (reply.grpc_status.ok()) {
    switch (reply.comm_status) {
    case SUCCESS:
      std::cout << "Command completed successfully\n";
      if (comm == "LIST") {
        std::cout << "All users: ";
        for (std::string room : reply.all_users) {
          std::cout << room << ", ";
        }
        std::cout << "\nFollowers: ";
        for (std::string room : reply.followers) {
          std::cout << room << ", ";
        }
        std::cout << std::endl;
      }
      break;
    case FAILURE_ALREADY_EXISTS:
      std::cout << "Input username already exists, command failed\n";
      break;
    case FAILURE_NOT_EXISTS:
      std::cout << "Input username does not exists, command failed\n";
      break;
    case FAILURE_INVALID_USERNAME:
      std::cout << "Command failed with invalid username\n";
      break;
    case FAILURE_NOT_A_FOLLOWER:
      std::cout << "Command failed with not a follower\n";
      break;     
    case FAILURE_INVALID:
      std::cout << "Command failed with invalid command\n";
      break;
    case FAILURE_UNKNOWN:
      std::cout << "Command failed with unknown reason\n";
      break;
    default:
      std::cout << "Invalid status\n";
      break;
    }
  } else {
    std::cout << "grpc failed: " << reply.grpc_status.error_message() << std::endl;
  }
}

void IClient::toUpperCase(std::string& str) const
{
  std::locale loc;
  for (std::string::size_type i = 0; i < str.size(); i++)
    str[i] = toupper(str[i], loc);
}

/*
 * get/displayPostMessage functions will be called in chatmode
 */
std::string getPostMessage()
{
  char buf[MAX_DATA];
  while (1) {
    fgets(buf, MAX_DATA, stdin);
    if (buf[0] != '\n')  break;
  }

  std::string message(buf);
  return message;
}

void displayPostMessage(const std::string& sender, const std::string& message, std::time_t& time)
{
  std::string t_str(std::ctime(&time));
  t_str[t_str.size()-1] = '\0';
  std::cout << sender << " (" << t_str << ") >> " << message << std::endl;
}

void displayReConnectionMessage(const std::string& host, const std::string & port) {
  std::cout << "Reconnecting to " << host << ":" << port << "..." << std::endl;
}
