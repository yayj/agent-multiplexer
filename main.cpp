#include <bit>
#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <range/v3/all.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

namespace asio  = boost::asio;
namespace fs    = std::filesystem;
namespace opts  = boost::program_options;
namespace views = ranges::views;

using namespace std::literals;
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

template <std::integral Int> asio::awaitable<Int> read_int(stream::socket& s)
{
  auto ret = Int{};
  co_await asio::async_read(
      s,
      asio::buffer(reinterpret_cast<char*>(&ret), sizeof(Int)),
      asio::use_awaitable
  );
  co_return std::byteswap(ret);
}

static asio::awaitable<size_t> read_chunk(stream::socket& s, std::span<char> b)
{
  auto len = co_await read_int<uint32_t>(s);
  if (ranges::size(b) < len + sizeof(uint32_t))
    throw std::range_error{"Buffer size is insufficient"};
  ranges::copy(
      views::counted(reinterpret_cast<char const*>(&len), sizeof(uint32_t)) | views::reverse,
      ranges::begin(b)
  );
  co_await asio::async_read(s, asio::buffer(b.subspan(sizeof(uint32_t)), len), asio::use_awaitable);
  co_return len + sizeof(uint32_t);
}

static asio::awaitable<std::string> read_id(stream::socket& s)
{
  auto ret = std::string(1024, 0);
  auto len = co_await read_chunk(s, ret);
  len += co_await read_chunk(s, std::span{ret}.subspan(len));
  ret.resize(len);
  co_return ret;
}

static asio::awaitable<std::vector<std::string>>
    request_ids(stream::socket s, stream::endpoint agent)
{
  co_await s.async_connect(agent, asio::use_awaitable);
  co_await asio::async_write(s, asio::buffer("\x00\x00\x00\x01\x0b"sv), asio::use_awaitable);

  co_await read_int<uint32_t>(s);
  if (co_await read_int<uint8_t>(s) != 0x0c)
    throw std::invalid_argument{"Invalid response from agent"};

  auto ret = std::vector<std::string>{};
  for (auto i = co_await read_int<uint32_t>(s); i > 0; --i) ret.emplace_back(co_await read_id(s));

  co_return ret;
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
