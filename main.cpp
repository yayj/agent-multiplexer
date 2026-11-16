#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>

namespace asio   = boost::asio;
namespace fs     = std::filesystem;
namespace opts   = boost::program_options;
namespace ranges = std::ranges;
namespace views  = std::views;

using stream = asio::local::stream_protocol;

static void daemonize(std::string_view pid, std::string_view log)
{
  // To be done.
}

static std::string replace_home(std::string_view orig)
{
  assert(!ranges::empty(orig));
  if (orig[0] != '~') return std::format("{}", orig);
  static auto home = std::string_view{::getenv("HOME")};
  return std::format("{}{}", home, orig.substr(1));
}

static asio::awaitable<void> do_session(stream::socket in)
{
  // To be done.
  co_return;
}

template <asio::execution::executor Executor, ranges::range Dirs>
asio::awaitable<void> run(Executor ex, Dirs&& dirs, stream::acceptor fe)
{
  while (true)
    asio::co_spawn(ex, do_session(co_await fe.async_accept(asio::use_awaitable)), asio::detached);
}

static void prepare_directory(std::string_view fn)
{
  auto path = fs::path{fn};
  if (fs::exists(path))
    throw std::system_error{
        std::make_error_code(std::errc::file_exists),
        std::format("{} exists", fn)
    };
  if (!fs::exists(path.parent_path())) fs::create_directories(path.parent_path());
}

int main(int argc, char* argv[])
{
  auto dirs = std::string{};
  auto fn   = std::string{};
  auto log  = std::string{};
  auto pid  = std::string{};

  // clang-format off
  auto desc = opts::options_description{"Available options"};
  desc.add_options()
    ("help,h", "Produce help message")
    (
     "backend,b",
     opts::value<std::string>(&dirs)->default_value("~/.ssh/agent,/tmp"),
     "Directories of back-end agents, separated by commas"
    )
    ("daemon,d", "Daemonize")
    (
     "frontend,f",
     opts::value<std::string>(&fn)->default_value("~/.ssh/agent.sock"),
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

  if (ranges::empty(fn)) fn = "~/.ssh/agent.sock";
  fn = replace_home(fn);

  prepare_directory(fn);

  if (args.count("daemon")) daemonize(pid, log);

  auto ctx = asio::io_context{};
  auto ex  = ctx.get_executor();
  asio::co_spawn(
      ex,
      run(ex,
          dirs | views::split(',') |
              views::filter([](auto&& item) { return !ranges::empty(item); }),
          stream::acceptor{ex, fn}),
      asio::detached
  );

  ctx.run();

  return 0;
}
