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

localhost での双方向通信の再現試験は、[ローカル組み合わせ試験手順](testing.md) を参照する。
