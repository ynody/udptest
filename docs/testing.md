# ローカル組み合わせ試験手順

## 1. 目的

同一マシン上で 2 台の UDP 通信サーバを起動し、それぞれに対応する UDP 送信指示クライアントから送信を指示して、UDP の相互通信を確認する。

この手順では、IPC 用 TCP 接続および UDP 通信のどちらも IPv4 ループバックアドレス `127.0.0.1` を使用する。

## 2. 前提条件

- UDP 通信サーバを 2 プロセス起動できること。
- UDP 送信指示クライアントを実行できること。
- UDP 待受ポートおよび IPC ポートが、他のプロセスと衝突しないこと。
- サーバ実行ファイル名は `udp_server`、クライアント実行ファイル名は `udp_sender` とする。

## 3. 設定ファイル

サーバ A 用とサーバ B 用に、別々の JSON 設定ファイルを作成する。以下の内容は `examples/server-a.json` および `examples/server-b.json` として同梱する。

### 3.1 サーバ A の設定例

`server-a.json`:

```json
{
  "listen_address": "127.0.0.1",
  "listen_port": 5000,
  "destination_address": "127.0.0.1",
  "destination_port": 5001,
  "ipc_port": 6000
}
```

### 3.2 サーバ B の設定例

`server-b.json`:

```json
{
  "listen_address": "127.0.0.1",
  "listen_port": 5001,
  "destination_address": "127.0.0.1",
  "destination_port": 5000,
  "ipc_port": 6001
}
```

この設定により、サーバ A から送信された UDP データグラムはサーバ B が受信し、サーバ B から送信された UDP データグラムはサーバ A が受信する。

## 4. UDP ペイロードファイル

例えば、次の内容で `packets-a.txt` を作成する。同じ内容の `examples/packets-a.txt` を同梱する。

```text
# Server A から Server B への試験パケット
DE AD BE EF
/Wait 100
01020304
```

このファイルは、4 バイトの UDP パケットを送信し、100 ミリ秒待機した後、別の 4 バイトの UDP パケットを送信することを表す。

サーバ B からサーバ A への試験を行う場合は、同様に `packets-b.txt` を用意する。試験用の `examples/packets-b.txt` も同梱する。

4,000バイト（hexstring 8,000文字）の長いUDPペイロード用として、`examples/packets-large.txt` も同梱する。再現用スクリプトはこのファイルをサーバ A からサーバ B へ送信し、サーバ B のパケットログに `Received(4000):` のダンプがペイロード全体と一致することを確認する。

このサイズは一般的なEthernet MTU（約1,500バイト）ではIPフラグメンテーションの対象となる。一方、localhost のループバックMTUが4,000バイトを超える環境ではフラグメントは発生しないため、再現用スクリプトは長いUDPデータグラムの送受信試験であり、IPフラグメントの発生そのものを保証する試験ではない。

## 5. 実行手順

異なるターミナルで、次の順に実行する。

macOS または Ubuntu Linux では、ビルド、2サーバの起動、双方向送信、受信ログ検査、およびサーバ停止を一括で行う [再現用スクリプト](../scripts/run_local_integration_test.sh) を使用できる。

```text
bash scripts/run_local_integration_test.sh
```

スクリプトは `build/server-a-integration.log` および `build/server-b-integration.log` にサーバ出力を保存する。Windows のネイティブ環境では、以下の手動手順を別々のコンソールで実行する。

1. サーバ A を起動する。

   ```text
   udp_server examples/server-a.json
   ```

2. サーバ B を起動する。

   ```text
   udp_server examples/server-b.json
   ```

3. サーバ A に対応するクライアントから、サーバ B 宛ての UDP 送信を指示する。

   ```text
   udp_sender examples/server-a.json examples/packets-a.txt
   ```

4. 必要に応じて、サーバ B に対応するクライアントからサーバ A 宛ての UDP 送信を指示する。

   ```text
   udp_sender examples/server-b.json examples/packets-b.txt
   ```

## 6. 期待結果

- `server-a.json` を渡したクライアントは、IPC ポート `6000` を通じてサーバ A へ送信を指示する。
- サーバ A は、設定された UDP 送信先 `127.0.0.1:5001`、すなわちサーバ B へ UDP データグラムを送信する。
- サーバ B の標準出力に、送信した各パケットに対応する受信概要が 1 行ずつ出力される。概要にはペイロードサイズとサーバ起動後の通し番号を含み、hex ダンプは含まれない。

出力例:

```text
[2026-08-18 12:34:56.789] [info] Received(4): 1
[2026-08-18 12:34:56.890] [info] Received(4): 2
```

- パケットログには、標準出力と同じ受信時刻・サイズ・通し番号に続けて、区切りなしの大文字hexダンプが出力される。設定例のように `packet_log_path` を省略した場合は、設定JSONと同じディレクトリに `udp_packets_YYYYMMDD-HHMMSS-mmm.log` 形式のログが生成される。

パケットログの例:

```text
[2026-08-18 12:34:56.789] [info] Received(4): 1: DEADBEEF
[2026-08-18 12:34:56.890] [info] Received(4): 2: 01020304
```

- クライアントは、各 UDP 送信についてサーバから成功応答を受け取った後、入力ファイルの全行を処理して終了する。
- 逆方向の試験では、サーバ A の標準出力に受信概要が、パケットログにhexダンプが出力される。

## 7. 停止

各サーバを起動したターミナルで `Ctrl+C` を入力して停止する。
