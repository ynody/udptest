#include <asio.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_sinks.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr std::uint32_t kMaxPayloadSize = 32'768;
constexpr std::uint32_t kMaxDelayMilliseconds = 1'800'000;
constexpr char kCommandPrefix = '/';
constexpr std::string_view kWaitCommandName = "Wait";
constexpr char kLogPattern[] = "[%Y-%m-%d %H:%M:%S.%e] [%l] %v";

/** @brief 利用者へのエラー表示を伴う送信指示クライアントの例外。 */
class ApplicationError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/** @brief 入力ファイルの1行から解釈したコマンド種別。 */
enum class CommandType {
    send_payload,
    delay,
};

/** @brief 入力ファイルの1行を解析して得た送信または待機コマンド。 */
struct InputCommand {
    CommandType type;
    std::vector<std::uint8_t> payload;
    std::uint32_t delay_milliseconds = 0;
};

using ErrorLogger = std::shared_ptr<spdlog::logger>;

/**
 * @brief 文字が空白文字かを判定する。
 * @param character 判定対象の文字。
 * @return 空白文字ならtrue。
 */
bool is_whitespace(const char character) {
    return std::isspace(static_cast<unsigned char>(character)) != 0;
}

/**
 * @brief 行が空白文字だけで構成されるかを判定する。
 * @param line 判定対象の行。
 * @return 空行または空白文字のみの行ならtrue。
 */
bool is_blank_line(std::string_view line) {
    for (const char character : line) {
        if (!is_whitespace(character)) {
            return false;
        }
    }
    return true;
}

/**
 * @brief ログメッセージを1行へ正規化する。
 * @param message 正規化するメッセージ。
 * @return 改行を空白に置換したメッセージ。
 */
std::string one_line(std::string message) {
    for (char& character : message) {
        if (character == '\r' || character == '\n') {
            character = ' ';
        }
    }
    return message;
}

/**
 * @brief 標準エラー出力へエラーログを出力する。
 * @param logger 使用するロガー。
 * @param message 出力するエラー内容。
 */
void log_error(const ErrorLogger& logger, std::string message) {
    logger->error("Error: {}", one_line(std::move(message)));
}

/**
 * @brief タイムスタンプ付きの標準エラー用ロガーを生成する。
 * @return 初期化済みのspdlogロガー。
 */
ErrorLogger create_error_logger() {
    auto logger = spdlog::stderr_logger_st("udp_sender_stderr");
    spdlog::set_pattern(kLogPattern);
    logger->set_level(spdlog::level::info);
    logger->flush_on(spdlog::level::err);
    return logger;
}

/**
 * @brief 設定JSONからIPC接続用TCPポートを読み取る。
 * @param settings_path 設定JSONファイルのパス。
 * @return 検証済みのIPCポート番号。
 * @throws ApplicationError ファイル、JSONまたはポート値が不正な場合。
 */
std::uint16_t read_ipc_port(const std::string& settings_path) {
    std::ifstream settings_file(settings_path);
    if (!settings_file) {
        throw ApplicationError("unable to open settings JSON file: " + settings_path);
    }

    nlohmann::json settings;
    try {
        settings_file >> settings;
    } catch (const nlohmann::json::exception& exception) {
        throw ApplicationError("invalid settings JSON: " + std::string(exception.what()));
    }

    if (!settings.is_object()) {
        throw ApplicationError("settings JSON root must be an object");
    }
    if (!settings.contains("ipc_port")) {
        throw ApplicationError("settings JSON is missing required key: ipc_port");
    }

    const auto& port_value = settings.at("ipc_port");
    std::uint64_t port = 0;
    if (port_value.is_number_unsigned()) {
        port = port_value.get<std::uint64_t>();
    } else if (port_value.is_number_integer()) {
        const auto signed_port = port_value.get<std::int64_t>();
        if (signed_port < 0) {
            throw ApplicationError("ipc_port must be an integer from 1 to 65535");
        }
        port = static_cast<std::uint64_t>(signed_port);
    } else {
        throw ApplicationError("ipc_port must be an integer from 1 to 65535");
    }

    if (port == 0 || port > std::numeric_limits<std::uint16_t>::max()) {
        throw ApplicationError("ipc_port must be an integer from 1 to 65535");
    }
    return static_cast<std::uint16_t>(port);
}

/**
 * @brief 16進数1桁を数値へ変換する。
 * @param character 変換対象の文字。
 * @return 0から15の値。16進数でない場合は-1。
 */
int hex_digit_value(const char character) {
    if (character >= '0' && character <= '9') {
        return character - '0';
    }
    if (character >= 'a' && character <= 'f') {
        return character - 'a' + 10;
    }
    if (character >= 'A' && character <= 'F') {
        return character - 'A' + 10;
    }
    return -1;
}

/**
 * @brief 空白を除いた16進文字列をUDPペイロードへ変換する。
 * @param line 入力ファイルのペイロード行。
 * @param line_number エラー表示に使う1始まりの行番号。
 * @return 復号済みペイロード。
 * @throws ApplicationError 16進表記または最大サイズが不正な場合。
 */
std::vector<std::uint8_t> parse_hex_payload(std::string_view line, std::size_t line_number) {
    std::vector<std::uint8_t> payload;
    payload.reserve(std::min<std::size_t>(line.size() / 2, kMaxPayloadSize));

    int high_nibble = -1;
    for (const char character : line) {
        if (is_whitespace(character)) {
            continue;
        }

        const int nibble = hex_digit_value(character);
        if (nibble < 0) {
            throw ApplicationError("line " + std::to_string(line_number) +
                                   ": invalid hexadecimal character");
        }

        if (high_nibble < 0) {
            if (payload.size() >= kMaxPayloadSize) {
                throw ApplicationError("line " + std::to_string(line_number) +
                                       ": decoded payload exceeds 32768 bytes");
            }
            high_nibble = nibble;
        } else {
            payload.push_back(static_cast<std::uint8_t>((high_nibble << 4) | nibble));
            high_nibble = -1;
        }
    }

    if (high_nibble >= 0) {
        throw ApplicationError("line " + std::to_string(line_number) +
                               ": hexadecimal payload has an odd number of digits");
    }

    return payload;
}

/**
 * @brief `/Wait` 特殊行の引数をミリ秒数へ変換する。
 * @param argument 入力ファイルの待機時間引数。
 * @param line_number エラー表示に使う1始まりの行番号。
 * @return 0から1800000までの待機時間（ミリ秒）。
 * @throws ApplicationError 数値表記または上限が不正な場合。
 */
std::uint32_t parse_wait_delay(std::string_view argument, std::size_t line_number) {
    std::uint64_t delay = 0;
    bool has_digit = false;

    for (const char character : argument) {
        if (is_whitespace(character)) {
            continue;
        }
        if (character < '0' || character > '9') {
            throw ApplicationError("line " + std::to_string(line_number) +
                                   ": delay must be a decimal integer in milliseconds");
        }

        has_digit = true;
        const auto digit = static_cast<std::uint64_t>(character - '0');
        if (delay > (kMaxDelayMilliseconds - digit) / 10) {
            throw ApplicationError("line " + std::to_string(line_number) +
                                   ": delay exceeds 1800000 milliseconds");
        }
        delay = delay * 10 + digit;
    }

    if (!has_digit) {
        throw ApplicationError("line " + std::to_string(line_number) +
                               ": delay is missing");
    }
    return static_cast<std::uint32_t>(delay);
}

/**
 * @brief `/` で始まる特殊行を待機コマンドへ解析する。
 * @param line 解析対象の特殊行。
 * @param line_number エラー表示に使う1始まりの行番号。
 * @return 解析済みの待機コマンド。
 * @throws ApplicationError コマンド名、区切り、または引数の書式が不正な場合。
 */
InputCommand parse_special_command(std::string_view line, std::size_t line_number) {
    std::size_t command_end = 1;
    while (command_end < line.size() && !is_whitespace(line[command_end])) {
        ++command_end;
    }

    const auto command_name = line.substr(1, command_end - 1);
    if (command_name.empty()) {
        throw ApplicationError("line " + std::to_string(line_number) +
                               ": special command name is missing");
    }
    if (command_name != kWaitCommandName) {
        throw ApplicationError("line " + std::to_string(line_number) +
                               ": unsupported special command");
    }
    if (command_end == line.size()) {
        throw ApplicationError("line " + std::to_string(line_number) +
                               ": wait delay is missing");
    }

    std::size_t argument_begin = command_end;
    while (argument_begin < line.size() && is_whitespace(line[argument_begin])) {
        ++argument_begin;
    }
    if (argument_begin == line.size()) {
        throw ApplicationError("line " + std::to_string(line_number) +
                               ": wait delay is missing");
    }

    return InputCommand{CommandType::delay,
                        {},
                        parse_wait_delay(line.substr(argument_begin), line_number)};
}

/**
 * @brief 入力ファイルの1行を送信、待機、または無視コマンドへ解析する。
 * @param line 解析対象の行。
 * @param line_number エラー表示に使う1始まりの行番号。
 * @return 空行・コメント行ではstd::nullopt、それ以外では実行コマンド。
 * @throws ApplicationError 行の書式が不正な場合。
 */
std::optional<InputCommand> parse_input_line(std::string_view line, std::size_t line_number) {
    if (is_blank_line(line) || line.front() == '#') {
        return std::nullopt;
    }

    if (line.front() == kCommandPrefix) {
        return parse_special_command(line, line_number);
    }

    return InputCommand{CommandType::send_payload, parse_hex_payload(line, line_number), 0};
}

/**
 * @brief UDP送信要求をIPCサーバーへ送り、結果応答を待つ。
 * @param socket 接続済みのループバックTCPソケット。
 * @param payload 送信を指示するUDPペイロード。
 * @throws ApplicationError IPC通信またはUDP送信指示が失敗した場合。
 */
void write_request(asio::ip::tcp::socket& socket,
                   const std::vector<std::uint8_t>& payload) {
    // IPCフレームの先頭には、ペイロード長をbig-endianの4バイトで付加する。
    std::array<std::uint8_t, 4> length_header{
        static_cast<std::uint8_t>((payload.size() >> 24) & 0xff),
        static_cast<std::uint8_t>((payload.size() >> 16) & 0xff),
        static_cast<std::uint8_t>((payload.size() >> 8) & 0xff),
        static_cast<std::uint8_t>(payload.size() & 0xff),
    };

    asio::error_code error;
    asio::write(socket, asio::buffer(length_header), error);
    if (error) {
        throw ApplicationError("failed to send IPC request length: " + error.message());
    }

    if (!payload.empty()) {
        asio::write(socket, asio::buffer(payload), error);
        if (error) {
            throw ApplicationError("failed to send IPC request payload: " + error.message());
        }
    }

    std::array<std::uint8_t, 1> response{};
    asio::read(socket, asio::buffer(response), error);
    if (error) {
        throw ApplicationError("failed to receive IPC response: " + error.message());
    }
    if (response[0] != 0) {
        throw ApplicationError("server reported UDP send failure (result code " +
                               std::to_string(response[0]) + ")");
    }

}

/**
 * @brief 入力ファイルを先頭から処理し、ペイロード送信と`/Wait`待機を順に実行する。
 * @param payload_path 16進ペイロードを記載した入力ファイルのパス。
 * @param socket IPCサーバーへ接続済みのTCPソケット。
 * @throws ApplicationError ファイル読み取りまたは行処理に失敗した場合。
 */
void process_payload_file(const std::string& payload_path, asio::ip::tcp::socket& socket) {
    std::ifstream payload_file(payload_path);
    if (!payload_file) {
        throw ApplicationError("unable to open UDP payload file: " + payload_path);
    }

    std::string line;
    std::size_t line_number = 0;
    while (std::getline(payload_file, line)) {
        ++line_number;
        const auto command = parse_input_line(line, line_number);
        if (!command.has_value()) {
            continue;
        }

        if (command->type == CommandType::delay) {
            // `/Wait` 特殊行は、次の送信指示を出す前にのみ適用する。
            std::this_thread::sleep_for(
                std::chrono::milliseconds(command->delay_milliseconds));
            continue;
        }

        write_request(socket, command->payload);
    }

    if (payload_file.bad()) {
        throw ApplicationError("failed while reading UDP payload file: " + payload_path);
    }
}

/**
 * @brief IPCサーバーへ接続し、入力ファイルに従ってUDP送信を指示する。
 * @param settings_path IPCポートを含む設定JSONファイルのパス。
 * @param payload_path 送信するペイロードを含む入力ファイルのパス。
 * @throws ApplicationError 接続または入力処理に失敗した場合。
 */
void run_sender(const std::string& settings_path, const std::string& payload_path) {
    const auto ipc_port = read_ipc_port(settings_path);

    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    const asio::ip::tcp::endpoint endpoint(asio::ip::address_v4::loopback(), ipc_port);

    asio::error_code error;
    socket.connect(endpoint, error);
    if (error) {
        throw ApplicationError("failed to connect to IPC server at 127.0.0.1:" +
                               std::to_string(ipc_port) + ": " + error.message());
    }

    process_payload_file(payload_path, socket);
}

}  // namespace

/**
 * @brief UDP送信指示クライアントのエントリーポイント。
 * @param argc コマンドライン引数の数。
 * @param argv 設定JSONとペイロードファイルを受け取る引数配列。
 * @return 成功時はEXIT_SUCCESS、失敗時はEXIT_FAILURE。
 */
int main(int argc, char* argv[]) {
    ErrorLogger logger;
    try {
        logger = create_error_logger();

        if (argc != 3) {
            throw ApplicationError("usage: udp_sender <settings-json> <udp-payload-file>");
        }

        run_sender(argv[1], argv[2]);
        return EXIT_SUCCESS;
    } catch (const std::exception& exception) {
        if (logger) {
            log_error(logger, exception.what());
        } else {
            std::fputs("Error: udp_sender failed before logging was initialized\n", stderr);
        }
        return EXIT_FAILURE;
    }
}
