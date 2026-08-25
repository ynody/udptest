#include <asio.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <array>
#include <cstdint>
#include <csignal>
#include <exception>
#include <fstream>
#include <functional>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using asio::ip::tcp;
using asio::ip::udp;
using json = nlohmann::json;

constexpr std::size_t kMaxPayloadSize = 32'768;
constexpr std::string_view kLogPattern = "[%Y-%m-%d %H:%M:%S.%e] [%l] %v";

std::shared_ptr<spdlog::logger> g_error_logger;

/** @brief サーバー用の標準出力・標準エラーログを初期化する。 */
void configure_logging() {
  auto output_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
  auto error_sink = std::make_shared<spdlog::sinks::stderr_sink_mt>();

  auto output_logger = std::make_shared<spdlog::logger>("output", output_sink);
  g_error_logger = std::make_shared<spdlog::logger>("errors", error_sink);
  spdlog::set_default_logger(std::move(output_logger));
  spdlog::register_logger(g_error_logger);
  spdlog::set_pattern(std::string{kLogPattern});
  spdlog::set_level(spdlog::level::info);
}

/**
 * @brief ログメッセージを1行で出力できる形式へ正規化する。
 * @param message 正規化するメッセージ。
 * @return 改行文字を空白に置換したメッセージ。
 */
std::string one_line(std::string message) {
  for (auto& character : message) {
    if (character == '\r' || character == '\n') {
      character = ' ';
    }
  }
  return message;
}

/**
 * @brief 標準エラー出力へエラーログを1行で記録する。
 * @param message 記録するエラー内容。
 */
void log_error(std::string message) {
  g_error_logger->error("Error: {}", one_line(std::move(message)));
}

/** @brief settings.json から読み込む通信設定。 */
struct Settings {
  asio::ip::address_v4 listen_address;
  std::uint16_t listen_port;
  asio::ip::address_v4 destination_address;
  std::uint16_t destination_port;
  std::uint16_t ipc_port;
};

/**
 * @brief JSONオブジェクトから有効なTCP/UDPポート番号を読み取る。
 * @param document 設定JSONオブジェクト。
 * @param key 読み取るキー名。
 * @return 1から65535までのポート番号。
 * @throws std::runtime_error 値が範囲外の場合。
 */
std::uint16_t read_port(const json& document, const char* key) {
  const auto value = document.at(key).get<std::int64_t>();
  if (value < 1 || value > 65'535) {
    throw std::runtime_error(std::string{key} + " must be in the range 1..65535");
  }
  return static_cast<std::uint16_t>(value);
}

/**
 * @brief 設定JSONファイルを読み込み、通信設定へ変換する。
 * @param path 設定JSONファイルのパス。
 * @return 検証済みの通信設定。
 * @throws std::exception ファイル、JSON、アドレスまたはポートが不正な場合。
 */
Settings load_settings(const std::string& path) {
  std::ifstream input{path};
  if (!input) {
    throw std::runtime_error("cannot open settings file: " + path);
  }

  json document;
  input >> document;
  if (!document.is_object()) {
    throw std::runtime_error("settings root must be a JSON object");
  }

  Settings settings{
      asio::ip::make_address_v4(document.at("listen_address").get<std::string>()),
      read_port(document, "listen_port"),
      asio::ip::make_address_v4(document.at("destination_address").get<std::string>()),
      read_port(document, "destination_port"),
      read_port(document, "ipc_port"),
  };
  return settings;
}

/**
 * @brief バイト列を大文字・区切りなしの16進文字列へ変換する。
 * @param data 変換対象の先頭アドレス。
 * @param size 変換対象のバイト数。
 * @return 16進ダンプ文字列。
 */
std::string to_hex(const std::uint8_t* data, const std::size_t size) {
  std::ostringstream output;
  output << std::hex << std::uppercase << std::setfill('0');
  for (std::size_t index = 0; index < size; ++index) {
    output << std::setw(2) << static_cast<unsigned int>(data[index]);
  }
  return output.str();
}

/** @brief 非同期UDP受信と、設定済み宛先への非同期UDP送信を担当する。 */
class UdpService {
 public:
  /**
   * @brief UDPソケットを作成し、受信アドレス・ポートへバインドする。
   * @param context 非同期I/Oコンテキスト。
   * @param settings 受信および送信先の設定。
   */
  UdpService(asio::io_context& context, const Settings& settings)
      : socket_(context,
                udp::endpoint{settings.listen_address, settings.listen_port}),
        destination_{settings.destination_address, settings.destination_port} {}

  /** @brief 非同期UDP受信を開始する。 */
  void start() { receive_next(); }

  /** @brief 保留中のUDP操作を中止し、UDPソケットを閉じる。 */
  void stop() {
    asio::error_code ignored;
    socket_.cancel(ignored);
    socket_.close(ignored);
  }

  /**
   * @brief 設定済み宛先へUDPペイロードを非同期送信する。
   * @tparam CompletionHandler `(error_code, size_t)` を受け取る完了ハンドラー型。
   * @param payload 送信するペイロード。完了まで所有権を保持する。
   * @param completion_handler 送信完了時に呼び出すハンドラー。
   */
  template <typename CompletionHandler>
  void send(std::shared_ptr<std::vector<std::uint8_t>> payload,
            CompletionHandler&& completion_handler) {
    // 関数引数の評価順序に依存しないよう、所有権をラムダへ移す前に
    // バッファを作成する。これにより、`std::move(payload)` が先に評価されて
    // 空になったshared_ptrを参照することを防ぐ。
    const auto payload_buffer = asio::buffer(*payload);

    // 非同期送信の完了までペイロードを保持する。
    socket_.async_send_to(
        payload_buffer, destination_,
        [payload = std::move(payload),
         completion_handler = std::forward<CompletionHandler>(completion_handler)](
            const asio::error_code& error, const std::size_t bytes_sent) mutable {
          completion_handler(error, bytes_sent);
        });
  }

 private:
  /** @brief 次のUDPデータグラムを受信し、完了後に受信を再開する。 */
  void receive_next() {
    socket_.async_receive_from(
        asio::buffer(receive_buffer_), sender_endpoint_,
        [this](const asio::error_code& error, const std::size_t bytes_received) {
          if (!error) {
            spdlog::info("Received({}): {}", bytes_received,
                         to_hex(receive_buffer_.data(), bytes_received));
          } else if (error != asio::error::operation_aborted) {
            log_error("UDP receive failed: " + error.message());
          }

          if (socket_.is_open()) {
            // 継続動作するサーバーとして、停止されるまで受信を繰り返す。
            receive_next();
          }
        });
  }

  udp::socket socket_;
  udp::endpoint destination_;
  udp::endpoint sender_endpoint_;
  std::array<std::uint8_t, kMaxPayloadSize> receive_buffer_{};
};

/** @brief 送信指示クライアント1接続分のTCP IPCプロトコルを処理する。 */
class IpcSession : public std::enable_shared_from_this<IpcSession> {
 public:
  /**
   * @brief 受け入れ済みTCPソケットにIPCセッションを関連付ける。
   * @param socket 接続済みのIPCソケット。
   * @param udp_service UDP送信を委譲するサービス。
   */
  IpcSession(tcp::socket socket, UdpService& udp_service)
      : socket_(std::move(socket)), udp_service_(udp_service) {}

  /** @brief IPCフレームの長さヘッダー読み取りを開始する。 */
  void start() { read_header(); }

  /** @brief セッションの保留中操作を中止し、TCP接続を閉じる。 */
  void stop() {
    asio::error_code ignored;
    socket_.cancel(ignored);
    socket_.shutdown(tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
  }

 private:
  /** @brief 次フレームの4バイト長さヘッダーを非同期で読み取る。 */
  void read_header() {
    // ハンドラー実行中もセッションを生存させる。
    auto self = shared_from_this();
    asio::async_read(
        socket_, asio::buffer(header_),
        [self](const asio::error_code& error, const std::size_t /*bytes_read*/) {
          if (error) {
            if (error != asio::error::eof &&
                error != asio::error::operation_aborted) {
              log_error("IPC header read failed: " + error.message());
            }
            self->stop();
            return;
          }
          self->handle_header();
        });
  }

  /** @brief 長さヘッダーを検証し、続くペイロードの読み取りを開始する。 */
  void handle_header() {
    // IPCプロトコルの長さはネットワークバイトオーダー（big-endian）。
    const auto payload_size =
        (static_cast<std::uint32_t>(header_[0]) << 24U) |
        (static_cast<std::uint32_t>(header_[1]) << 16U) |
        (static_cast<std::uint32_t>(header_[2]) << 8U) |
        static_cast<std::uint32_t>(header_[3]);

    if (payload_size > kMaxPayloadSize) {
      log_error("IPC payload exceeds 32768 bytes");
      write_response(1, true);
      return;
    }

    payload_ = std::make_shared<std::vector<std::uint8_t>>(payload_size);
    if (payload_->empty()) {
      send_udp();
      return;
    }

    auto self = shared_from_this();
    asio::async_read(
        socket_, asio::buffer(*payload_),
        [self](const asio::error_code& error, const std::size_t /*bytes_read*/) {
          if (error) {
            if (error != asio::error::operation_aborted) {
              log_error("IPC payload read failed: " + error.message());
            }
            self->stop();
            return;
          }
          self->send_udp();
        });
  }

  /** @brief 読み取り済みのIPCペイロードをUDPで送信する。 */
  void send_udp() {
    auto self = shared_from_this();
    const auto expected_size = payload_->size();
    udp_service_.send(
        payload_,
        [self, expected_size](const asio::error_code& error,
                              const std::size_t bytes_sent) {
          if (error || bytes_sent != expected_size) {
            const auto detail = error ? error.message() : "short UDP send";
            log_error("UDP send failed: " + detail);
            self->write_response(1, false);
            return;
          }
          self->write_response(0, false);
        });
  }

  /**
   * @brief IPCクライアントへ送信結果の1バイト応答を書き込む。
   * @param result 0は成功、0以外は失敗を表す結果コード。
   * @param close_after_write 応答後にセッションを閉じる場合はtrue。
   */
  void write_response(const std::uint8_t result, const bool close_after_write) {
    response_[0] = result;
    auto self = shared_from_this();
    asio::async_write(
        socket_, asio::buffer(response_),
        [self, close_after_write](const asio::error_code& error,
                                  const std::size_t /*bytes_written*/) {
          if (error) {
            if (error != asio::error::operation_aborted) {
              log_error("IPC response write failed: " + error.message());
            }
            self->stop();
            return;
          }
          if (close_after_write) {
            self->stop();
          } else {
            self->read_header();
          }
        });
  }

  tcp::socket socket_;
  UdpService& udp_service_;
  std::array<std::uint8_t, 4> header_{};
  std::array<std::uint8_t, 1> response_{};
  std::shared_ptr<std::vector<std::uint8_t>> payload_;
};

/** @brief ループバックTCPでIPC接続を受け付けるサーバー。 */
class IpcServer {
 public:
  /**
   * @brief ループバックアドレスの指定ポートでIPC待受を初期化する。
   * @param context 非同期I/Oコンテキスト。
   * @param port 待受TCPポート。
   * @param udp_service 接続ごとのUDP送信先サービス。
   */
  IpcServer(asio::io_context& context, const std::uint16_t port,
            UdpService& udp_service)
      : acceptor_(context), udp_service_(udp_service) {
    const tcp::endpoint endpoint{asio::ip::address_v4::loopback(), port};
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();
  }

  /** @brief 非同期TCP acceptループを開始する。 */
  void start() { accept_next(); }

  /** @brief 保留中のaccept操作を中止し、待受ソケットを閉じる。 */
  void stop() {
    asio::error_code ignored;
    acceptor_.cancel(ignored);
    acceptor_.close(ignored);
  }

 private:
  /** @brief 次のIPCクライアント接続を受け付け、待受を継続する。 */
  void accept_next() {
    acceptor_.async_accept([this](const asio::error_code& error,
                                  tcp::socket socket) {
      if (!error) {
        std::make_shared<IpcSession>(std::move(socket), udp_service_)->start();
      } else if (error != asio::error::operation_aborted) {
        log_error("IPC accept failed: " + error.message());
      }

      if (acceptor_.is_open()) {
        accept_next();
      }
    });
  }

  tcp::acceptor acceptor_;
  UdpService& udp_service_;
};

/**
 * @brief UDP通信とTCP IPCを起動し、SIGINTまでイベントループを実行する。
 * @param settings 起動に使用する通信設定。
 * @return 正常終了時は0。
 */
int run(const Settings& settings) {
  asio::io_context context;
  UdpService udp_service{context, settings};
  IpcServer ipc_server{context, settings.ipc_port, udp_service};
  asio::signal_set signals{context, SIGINT};

  signals.async_wait([&](const asio::error_code& error, const int /*signal*/) {
    if (error) {
      return;
    }
    // Ctrl+Cではすべての待受を閉じてio_contextを停止する。
    ipc_server.stop();
    udp_service.stop();
    context.stop();
  });

  udp_service.start();
  ipc_server.start();
  context.run();
  return 0;
}

}  // namespace

/**
 * @brief UDPサーバーのエントリーポイント。
 * @param argc コマンドライン引数の数。
 * @param argv コマンドライン引数。設定JSONを1つ受け取る。
 * @return 成功時は0、引数または起動エラー時は1。
 */
int main(const int argc, char* argv[]) {
  configure_logging();
  if (argc != 2) {
    log_error("usage: udp_server <settings-json>");
    return 1;
  }

  try {
    return run(load_settings(argv[1]));
  } catch (const std::exception& error) {
    log_error(error.what());
    return 1;
  }
}
