#include <boost/program_options.hpp>
#include <iostream>
#include <string>

namespace opts = boost::program_options;

int main(int argc, char* argv[])
{
  auto be  = std::string{};
  auto fe  = std::string{};
  auto log = std::string{};
  auto pid = std::string{};

  // clang-format off
  auto desc = opts::options_description{"Available options"};
  desc.add_options()
    ("help,h", "Produce help message")
    (
     "backend,b",
     opts::value<std::string>(&be)->default_value("~/.ssh/agent,/tmp"),
     "Directories of back-end agents, separated by commas"
     )
    ("daemon,d", "Daemonize")
    (
     "frontend,f",
     opts::value<std::string>(&fe)->default_value("~/.ssh/agent.sock"),
     "Fixed socket to accept connections"
    )
    (
     "log-file,l",
     opts::value<std::string>(&log)->default_value("/dev/null"),
     "Log file, enabled only if --daemon option specified"
    )
    (
     "pid-file,p",
     opts::value<std::string>(&pid)->default_value("/dev/null"),
     "PID file, enabled only if --daemon option specified"
    );
  // clang-format on

  auto args = opts::variables_map{};
  opts::store(opts::parse_command_line(argc, argv, desc), args);
  opts::notify(args);

  if (args.count("help")) {
    std::cout << desc;
    return 1;
  }

  return 0;
}
