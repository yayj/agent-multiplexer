#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <ranges>
#include <string>

namespace asio   = boost::asio;
namespace fs     = std::filesystem;
namespace opts   = boost::program_options;
namespace ranges = std::ranges;
namespace views  = std::views;

static void daemonize(std::string_view pid, std::string_view log)
{
  // To be done.
}

template <asio::execution::executor Executor, ranges::range Dirs>
asio::awaitable<void> run(Executor ex, Dirs&& dirs, fs::path const& fe)
{
  // To be done.
  co_return;
}

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

  if (args.count("daemon")) daemonize(pid, log);

  auto ctx = asio::io_context{};
  asio::co_spawn(ctx, run(ctx.get_executor(), be | views::split(','), fe), asio::detached);

  ctx.run();

  return 0;
}
