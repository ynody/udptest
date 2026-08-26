#include <asio.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <csignal>
#include <ctime>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using asio::ip::tcp;
using asio::ip::udp;
using json = nlohmann::json;

constexpr std::size_t kMaxPayloadSize = 32'768;
constexpr std::size_t kDefaultPacketLogQueueLimitBytes = 8 * 1024 * 1024;
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
  std::filesystem::path packet_log_path;
  std::size_t packet_log_queue_limit_bytes;
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
 * @brief 正の整数として指定された出力キュー上限を読み取る。
 * @param document 設定JSONオブジェクト。
 * @param key 読み取るキー名。
 * @return std::size_tで表現可能な正のキュー上限。
 * @throws std::runtime_error 値が正の整数でない、または範囲外の場合。
 */
std::size_t read_positive_size(const json& document, const char* key) {
  const auto& value = document.at(key);
  std::uint64_t unsigned_value = 0;
  if (value.is_number_unsigned()) {
    unsigned_value = value.get<std::uint64_t>();
  } else if (value.is_number_integer()) {
    const auto signed_value = value.get<std::int64_t>();
    if (signed_value <= 0) {
      throw std::runtime_error(std::string{key} + " must be a positive integer");
    }
    unsigned_value = static_cast<std::uint64_t>(signed_value);
  } else {
    throw std::runtime_error(std::string{key} + " must be a positive integer");
  }

  if (unsigned_value == 0 ||
      unsigned_value > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    throw std::runtime_error(std::string{key} + " must be a positive integer");
  }
  return static_cast<std::size_t>(unsigned_value);
}

/**
 * @brief std::chronoの時刻をローカル時刻のファイル名部分へ変換する。
 * @param time 起動時刻。
 * @return `YYYYMMDD-HHMMSS-mmm`形式の時刻文字列。
 */
std::string format_filename_timestamp(const std::chrono::system_clock::time_point time) {
  const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(time);
  const auto milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(time - seconds).count();
  const auto time_t_value = std::chrono::system_clock::to_time_t(seconds);

  std::tm local_time{};
#if defined(_WIN32)
  if (localtime_s(&local_time, &time_t_value) != 0) {
    throw std::runtime_error("failed to convert startup time to local time");
  }
#else
  if (localtime_r(&time_t_value, &local_time) == nullptr) {
    throw std::runtime_error("failed to convert startup time to local time");
  }
#endif

  std::ostringstream output;
  output << std::put_time(&local_time, "%Y%m%d-%H%M%S-") << std::setfill('0')
         << std::setw(3) << milliseconds;
  return output.str();
}

/**
 * @brief 自動生成ログファイル名を作成する。
 * @param settings_directory 設定JSONファイルの配置ディレクトリ。
 * @return 既存ファイルと重複しないログファイルパス。
 */
std::filesystem::path generate_packet_log_path(
    const std::filesystem::path& settings_directory) {
  const auto stem = std::string{"udp_packets_"} +
                    format_filename_timestamp(std::chrono::system_clock::now());
  for (std::size_t suffix = 0;; ++suffix) {
    const auto suffix_text = suffix == 0 ? std::string{} : "-" + std::to_string(suffix);
    const auto candidate = settings_directory / (stem + suffix_text + ".log");
    std::error_code error;
    const bool exists = std::filesystem::exists(candidate, error);
    if (error) {
      throw std::runtime_error("cannot inspect packet log path: " + error.message());
    }
    if (!exists) {
      return candidate;
    }
  }
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

  std::error_code path_error;
  const auto settings_file_path = std::filesystem::absolute(path, path_error);
  if (path_error) {
    throw std::runtime_error("cannot resolve settings file path: " + path_error.message());
  }
  const auto settings_directory = settings_file_path.parent_path();

  std::filesystem::path packet_log_path;
  if (document.contains("packet_log_path")) {
    const auto& path_value = document.at("packet_log_path");
    if (!path_value.is_string()) {
      throw std::runtime_error("packet_log_path must be a non-empty string");
    }
    const auto path_text = path_value.get<std::string>();
    if (path_text.empty()) {
      throw std::runtime_error("packet_log_path must be a non-empty string");
    }
    packet_log_path = std::filesystem::path{path_text};
    if (packet_log_path.is_relative()) {
      packet_log_path = settings_directory / packet_log_path;
    }
    packet_log_path = packet_log_path.lexically_normal();
  } else {
    packet_log_path = generate_packet_log_path(settings_directory);
  }

  const auto packet_log_queue_limit_bytes =
      document.contains("packet_log_queue_limit_bytes")
          ? read_positive_size(document, "packet_log_queue_limit_bytes")
          : kDefaultPacketLogQueueLimitBytes;

  Settings settings{
      asio::ip::make_address_v4(document.at("listen_address").get<std::string>()),
      read_port(document, "listen_port"),
      asio::ip::make_address_v4(document.at("destination_address").get<std::string>()),
      read_port(document, "destination_port"),
      read_port(document, "ipc_port"),
      std::move(packet_log_path),
      packet_log_queue_limit_bytes,
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

/** @brief 出力専用スレッドへ渡す受信パケット情報。 */
struct PacketLogRecord {
  spdlog::log_clock::time_point receive_time;
  std::uint64_t sequence;
  std::vector<std::uint8_t> payload;
};

/**
 * @brief UDP受信記録を非同期に標準出力およびファイルへ出力する。
 *
 * キューのミューテックスを保持するのは記録の移動とキュー長更新だけであり、
 * hex変換・spdlog呼び出し・ファイルI/Oは出力スレッドがミューテックス外で行う。
 */
class PacketLogger {
 public:
  /**
   * @brief ログファイルを開き、出力先を初期化する。
   * @param path 受信パケットログの絶対または解決済みパス。
   * @param queue_limit_bytes キューが保持できるペイロード合計バイト数。
   * @throws std::exception ログファイルを開けない場合。
   */
  PacketLogger(const std::filesystem::path& path, const std::size_t queue_limit_bytes)
      : output_logger_(spdlog::default_logger()),
        file_logger_(std::make_shared<spdlog::logger>(
            "packet-file", std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                               path.string(), false))),
        queue_limit_bytes_(queue_limit_bytes) {
    file_logger_->set_pattern(std::string{kLogPattern});
    file_logger_->set_level(spdlog::level::info);
  }

  PacketLogger(const PacketLogger&) = delete;
  PacketLogger& operator=(const PacketLogger&) = delete;

  /** @brief 出力専用スレッドを開始する。 */
  void start() { worker_ = std::thread([this] { output_loop(); }); }

  /**
   * @brief 受信記録を待ち行列へ追加する。
   *
   * キュー上限を超える場合は保留中の記録を破棄する。今回の記録が上限以内なら
   * キューをクリアした後に受け付けるが、今回の記録だけで上限を超える場合は
   * その記録も破棄件数へ加えてキューへ追加しない。
   *
   * @param receive_time 受信完了時に採取した時刻。
   * @param sequence サーバ起動からの受信通し番号。
   * @param payload 受信したペイロード。所有権を移動する。
   */
  void enqueue(const spdlog::log_clock::time_point receive_time,
               const std::uint64_t sequence,
               std::vector<std::uint8_t> payload) {
    bool enqueue_record = true;
    {
      std::lock_guard<std::mutex> lock{mutex_};
      if (stopping_) {
        return;
      }

      const bool queue_would_exceed =
          queue_bytes_ > queue_limit_bytes_ ||
          payload.size() > queue_limit_bytes_ - queue_bytes_;
      if (queue_would_exceed) {
        discarded_count_ += queue_.size();
        queue_.clear();
        queue_bytes_ = 0;

        if (payload.size() > queue_limit_bytes_) {
          ++discarded_count_;
          // 上限を超える単独レコードは厳密なキュー上限を維持するため破棄する。
          enqueue_record = false;
        }
      }

      if (enqueue_record) {
        queue_bytes_ += payload.size();
        queue_.push_back(
            PacketLogRecord{receive_time, sequence, std::move(payload)});
      }
    }
    condition_.notify_one();
  }

  /**
   * @brief 保留中の記録をすべて出力し、ファイルをflushしてスレッドを停止する。
   */
  void stop() {
    {
      std::lock_guard<std::mutex> lock{mutex_};
      if (stopping_) {
        // stop() は複数回呼び出しても安全にする。
        return;
      }
      stopping_ = true;
    }
    condition_.notify_one();

    if (worker_.joinable()) {
      worker_.join();
    }
    file_logger_->flush();
  }

  /** @brief デストラクタで保留中の記録を破棄せず停止する。 */
  ~PacketLogger() { stop(); }

 private:
  /** @brief キューから記録を取り出し、警告および記録を出力する。 */
  void output_loop() {
    for (;;) {
      PacketLogRecord record;
      std::size_t discarded_count = 0;
      bool has_record = false;
      bool should_exit = false;

      {
        std::unique_lock<std::mutex> lock{mutex_};
        condition_.wait(lock, [this] {
          return stopping_ || !queue_.empty() || discarded_count_ != 0;
        });

        discarded_count = discarded_count_;
        discarded_count_ = 0;
        if (!queue_.empty()) {
          record = std::move(queue_.front());
          queue_.pop_front();
          queue_bytes_ -= record.payload.size();
          has_record = true;
        } else if (stopping_) {
          should_exit = true;
        }
      }

      if (discarded_count != 0) {
        emit_overflow_warning(discarded_count);
      }
      if (has_record) {
        emit_record(std::move(record));
      }
      if (should_exit) {
        return;
      }
    }
  }

  /** @brief キュー破棄警告を標準エラーおよびパケットログへ出力する。 */
  void emit_overflow_warning(const std::size_t discarded_count) {
    try {
      const auto warning_time = spdlog::log_clock::now();
      const auto message =
          std::string{"Warning: packet log queue overflow; discarded "} +
          std::to_string(discarded_count) + " packet logs";
      g_error_logger->log(warning_time, {}, spdlog::level::warn, message);
      file_logger_->log(warning_time, {}, spdlog::level::warn, message);
    } catch (const std::exception& error) {
      log_error("packet log overflow warning failed: " + std::string{error.what()});
    } catch (...) {
      log_error("packet log overflow warning failed: unknown exception");
    }
  }

  /** @brief 受信時刻付きの概要とhexダンプを両出力先へ記録する。 */
  void emit_record(PacketLogRecord record) {
    try {
      const auto summary = std::string{"Received("} +
                           std::to_string(record.payload.size()) + "): " +
                           std::to_string(record.sequence);
      const auto hex = to_hex(record.payload.data(), record.payload.size());
      const auto file_record = summary + ": " + hex;

      output_logger_->log(record.receive_time, {}, spdlog::level::info, summary);
      file_logger_->log(record.receive_time, {}, spdlog::level::info, file_record);
    } catch (const std::exception& error) {
      log_error("packet log output failed: " + std::string{error.what()});
    } catch (...) {
      log_error("packet log output failed: unknown exception");
    }
  }

  std::shared_ptr<spdlog::logger> output_logger_;
  std::shared_ptr<spdlog::logger> file_logger_;
  const std::size_t queue_limit_bytes_;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<PacketLogRecord> queue_;
  std::size_t queue_bytes_ = 0;
  std::size_t discarded_count_ = 0;
  bool stopping_ = false;
  std::thread worker_;
};

/** @brief 非同期UDP受信と、設定済み宛先への非同期UDP送信を担当する。 */
class UdpService {
 public:
  /**
   * @brief UDPソケットを作成し、受信アドレス・ポートへバインドする。
   * @param context 非同期I/Oコンテキスト。
   * @param settings 受信および送信先の設定。
   * @param packet_logger 受信記録を非同期出力するロガー。
   */
  UdpService(asio::io_context& context, const Settings& settings,
             PacketLogger& packet_logger)
      : socket_(context,
                udp::endpoint{settings.listen_address, settings.listen_port}),
        destination_{settings.destination_address, settings.destination_port},
        packet_logger_(packet_logger) {}

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
            const auto receive_time = spdlog::log_clock::now();
            const auto sequence = next_sequence_++;
            std::vector<std::uint8_t> payload(receive_buffer_.begin(),
                                              receive_buffer_.begin() + bytes_received);
            packet_logger_.enqueue(receive_time, sequence, std::move(payload));
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
  PacketLogger& packet_logger_;
  udp::endpoint sender_endpoint_;
  std::array<std::uint8_t, kMaxPayloadSize> receive_buffer_{};
  std::uint64_t next_sequence_ = 1;
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
  // ログファイルを先に開くため、失敗時にUDP/TCPサービスを起動しない。
  PacketLogger packet_logger{settings.packet_log_path,
                             settings.packet_log_queue_limit_bytes};
  packet_logger.start();
  UdpService udp_service{context, settings, packet_logger};
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
  packet_logger.stop();
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
