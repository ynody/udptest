# 導入・配布手順

## 1. 方針

Asio、nlohmann/json、および spdlog はすべてヘッダオンリー版を使用する。ライブラリ本体は Git 管理せず、依存ライブラリ取得スクリプトにより `third_party/` へダウンロードする。

`third_party/` は `.gitignore` の対象であり、Git リポジトリへコミットしない。

## 2. 依存ライブラリの取得

プロジェクトルートで次のコマンドを実行する。

```text
cmake -P scripts/download_dependencies.cmake
```

このスクリプトは、固定したリリースのアーカイブを公式 GitHub リポジトリからダウンロードし、SHA-256 チェックサムを検証したうえで、必要なヘッダファイルとライセンス文書を次の場所に展開する。

| ライブラリ | バージョン | 展開先 |
| --- | --- | --- |
| Asio（スタンドアロン版） | 1.38.2 | `third_party/asio/include` |
| nlohmann/json | 3.12.0 | `third_party/nlohmann_json/include` |
| spdlog | 1.17.0 | `third_party/spdlog/include` |

同じバージョンが既に配置されている場合、スクリプトはダウンロードを行わない。バージョンを更新する場合は、スクリプトと本書、ならびに `libraries.md` を同時に更新する。

## 3. ビルド

依存ライブラリ取得後、CMake と Ninja を用いてビルドする。

```text
cmake -S . -B build -G Ninja
cmake --build build
```

CMake 設定は、上記の `third_party/*/include` をインクルードパスへ追加する。

## 4. 配布

ヘッダオンリーライブラリはビルド時にコンパイルされるため、実行時に `third_party/` を配布する必要はない。

配布物には、ビルド済みの UDP 通信サーバ実行ファイル、UDP 送信指示クライアント実行ファイル、利用者が指定する JSON 設定ファイル、および各依存ライブラリのライセンス文書を含める。

実行時は、サーバへ設定JSONファイルのパスを渡し、クライアントへ同じ設定JSONファイルのパスと UDP ペイロードファイルのパスを渡す。

```text
udp_server <設定JSONファイル>
udp_sender <設定JSONファイル> <UDPペイロードファイル>
```

## 5. パケットログ出力の設定

`udp_server` は、受信パケットのサイズと通し番号を標準出力へ表示し、全ペイロードのhexダンプをテキストログファイルへ記録する。出力先と出力待ちキューの上限は、サーバの設定JSONに任意で指定できる。

```json
{
  "listen_address": "127.0.0.1",
  "listen_port": 5000,
  "destination_address": "192.0.2.10",
  "destination_port": 5001,
  "ipc_port": 6000,
  "packet_log_path": "logs/received.log",
  "packet_log_queue_limit_bytes": 8388608
}
```

- `packet_log_path` は受信パケットログの出力先である。相対パスは設定JSONファイルの配置ディレクトリを基準に解決する。指定したファイルが既にある場合は追記する。省略時は、設定JSONと同じディレクトリに `udp_packets_YYYYMMDD-HHMMSS-mmm.log` 形式の新規ファイルを自動生成する。
- `packet_log_queue_limit_bytes` は、出力待ちキューに保持するUDPペイロード合計の上限バイト数である。省略時は8 MiB（`8388608`）を使用する。出力処理が受信より遅く上限に達した場合、保留中の画面・ファイル記録を破棄して `warn` ログを出力するが、UDP受信処理は継続する。

ログファイルを作成または追記できない場合、サーバは起動時にエラーを表示して終了する。ログファイルのローテーションは行わない。

localhost での双方向通信の再現試験は、[ローカル組み合わせ試験手順](testing.md) を参照する。
