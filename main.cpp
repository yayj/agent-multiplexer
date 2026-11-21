#include <bit>
#include <boost/asio.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <range/v3/all.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>

namespace asio  = boost::asio;
namespace fs    = std::filesystem;
namespace opts  = boost::program_options;
namespace views = ranges::views;

using namespace std::literals;

using stream = asio::local::stream_protocol;
using asio::experimental::make_parallel_group;
using asio::experimental::wait_for_all;
using asio::experimental::wait_for_one;

static void daemonize(std::string_view pid, std::string_view log)
{
  // To be done.
}

static bool belongs_to_me(fs::path const& location)
{
  using Stat = struct ::stat;
  auto fst   = Stat{};
  return ::stat(location.c_str(), &fst) == 0 && fst.st_uid == ::getuid();
}

static std::string replace_home(std::string_view orig)
{
  assert(!ranges::empty(orig));
  if (orig[0] != '~') return std::format("{}", orig);
  static auto home = std::string_view{::getenv("HOME")};
  return std::format("{}{}", home, orig.substr(1));
}

template <std::integral Int, std::output_iterator<char> Iter> void write_int(Int data, Iter& it)
{
  ranges::copy(
      views::counted(reinterpret_cast<char const*>(&data), sizeof(Int)) | views::reverse,
      it
  );
  std::advance(it, sizeof(Int));
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

static asio::awaitable<std::pair<std::string, std::string>> read_id(stream::socket& s)
{
  auto identity = std::string(1024, 0);
  identity.resize(co_await read_chunk(s, identity));
  auto comment = std::string(1024, 0);
  comment.resize(co_await read_chunk(s, comment));
  co_return std::make_pair(std::move(identity), std::move(comment));
}

static asio::awaitable<std::unordered_map<std::string, std::string>>
    request_ids(stream::socket s, stream::endpoint agent)
{
  co_await s.async_connect(agent, asio::use_awaitable);
  co_await asio::async_write(s, asio::buffer("\x00\x00\x00\x01\x0b"sv), asio::use_awaitable);

  co_await read_int<uint32_t>(s);
  if (co_await read_int<uint8_t>(s) != 0x0c)
    throw std::invalid_argument{"Invalid response from agent"};

  auto ret = std::unordered_map<std::string, std::string>{};
  for (auto i = co_await read_int<uint32_t>(s); i > 0; --i) ret.emplace(co_await read_id(s));

  co_return ret;
}

class Backend {
public:
  template <asio::execution::executor Executor>
  Backend(Executor ex) : strand_{asio::make_strand(ex)}
  {
  }

  asio::awaitable<void> start(std::string_view names)
  {
    auto ex    = strand_.get_inner_executor();
    auto timer = asio::system_timer{ex};
    auto paths = names | views::split(',') |
                 views::filter([](auto&& piece) { return !ranges::empty(piece); }) |
                 views::transform([](auto&& piece) {
                   return std::string_view{
                       std::addressof(*ranges::begin(piece)),
                       static_cast<size_t>(ranges::distance(piece))
                   };
                 }) |
                 views::transform([](auto&& name) { return fs::path{replace_home(name)}; });
    while (true) {
      auto subs = paths | views::filter([](auto&& p) { return fs::is_directory(p); }) |
                  views::transform([](auto&& p) {
                    return fs::recursive_directory_iterator{
                        p,
                        fs::directory_options::skip_permission_denied
                    };
                  }) |
                  views::join | views::transform([](auto&& entry) { return entry.path(); });
      auto agents = views::concat(subs, paths) |
                    views::filter([](auto&& p) { return fs::is_socket(p) && belongs_to_me(p); }) |
                    views::transform([](auto&& p) { return p.string(); });
      auto tasks =
          make_parallel_group(ranges::to<std::vector>(agents | views::transform([=](auto&& name) {
                                                        return asio::co_spawn(
                                                            ex,
                                                            request_ids(stream::socket{ex}, name),
                                                            asio::deferred
                                                        );
                                                      })));
      timer.expires_after(1s);
      auto [oo, io, eptrs, rs, ec] = co_await make_parallel_group(
                                         tasks.async_wait(wait_for_all(), asio::deferred),
                                         timer.async_wait(asio::deferred)
      )
                                         .async_wait(wait_for_one(), asio::use_awaitable);

      auto data =
          views::zip(eptrs, rs | views::move, agents) |
          views::filter([](auto&& item) { return std::get<0>(item) == nullptr; }) |
          views::transform([](auto&& item) {
            return std::get<1>(item) | views::move |
                   views::transform([fn = std::get<2>(item)](auto&& k) {
                     auto [id, comment] = std::move(k);
                     return std::make_pair(std::move(id), std::make_pair(std::move(comment), fn));
                   });
          }) |
          views::join |
          ranges::to<std::unordered_map<std::string, std::pair<std::string, std::string>>>();

      co_await asio::post(asio::bind_executor(strand_, asio::use_awaitable));
      std::swap(data, data_);

      timer.expires_after(10s);
      co_await timer.async_wait(asio::use_awaitable);
    }
  }

  asio::awaitable<std::string> list()
  {
    co_await asio::post(asio::bind_executor(strand_, asio::use_awaitable));

    auto ret = ranges::fold_left(
        data_ | views::transform([](auto&& pair) {
          return std::format("{}{}", pair.first, pair.second.first);
        }),
        std::string(1 + 2 * sizeof(uint32_t), 0),
        [](auto acc, auto&& id) { return acc.append(id); }
    );

    auto it = ranges::begin(ret);
    write_int<uint32_t>(ranges::size(ret) - 4, it);
    write_int<char>(0x0c, it);
    write_int<uint32_t>(ranges::size(data_), it);

    co_return ret;
  }

private:
  asio::strand<asio::any_io_executor>                                  strand_;
  std::unordered_map<std::string, std::pair<std::string, std::string>> data_;
};

static asio::awaitable<void> do_session(stream::socket in, Backend& be)
{
  auto len  = co_await read_int<uint32_t>(in);
  auto code = co_await read_int<uint8_t>(in);
  switch (code) {
  case 0x0b:
    co_await asio::async_write(in, asio::buffer(co_await be.list()), asio::use_awaitable);
    break;
  default:
    std::cout << std::format("Unsupported code: {:02x}\n", code);
    co_await asio::async_write(in, asio::buffer("\x00\x00\x00\x01\x05"), asio::use_awaitable);
    break;
  }
}

static asio::awaitable<void> run(stream::acceptor fe, Backend& be)
{
  while (true)
    asio::co_spawn(
        fe.get_executor(),
        do_session(co_await fe.async_accept(asio::use_awaitable), be),
        asio::detached
    );
}

template <asio::execution::executor Executor>
asio::awaitable<void> tear_down(Executor ex, std::string_view fn)
{
  auto sig = asio::signal_set{ex};
  sig.add(SIGTERM);
  sig.add(SIGINT);
  co_await sig.async_wait(asio::use_awaitable);
  fs::remove(fn);
  exit(1);
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

  auto be = Backend{ex};
  asio::co_spawn(ex, be.start(dirs), asio::detached);
  asio::co_spawn(ex, run(stream::acceptor{ex, fn}, be), asio::detached);
  asio::co_spawn(ex, tear_down(ex, fn), asio::detached);

  ctx.run();

  return 0;
}
