# AetherMesh（日本語）

**ギガバイトを計算機へ送るのをやめて、計算機のほうを送る。**

AetherMesh は、既存の実行環境 — AWS・GCP・Azure・VPS・ベアメタル・机の下の PC・Raspberry Pi — の **上に載せる** Rust 製のレイヤーです。やることは 2 つだけ。

- **どのノードでタスクを走らせるか** を、負荷・レイテンシ・帯域・データの所在から決める
- **そのために何バイト動かす必要があるか** を最小化する

タスクは WebAssembly として実行できるので、処理は TypeScript・Rust・Go など WASM に出せる言語で書けます。投入は Node / Python / Go の SDK から。

> English: [README.md](README.md)

---

## 考え方

分散システムは普通、**データをコードのある場所へ運びます**。データが大きく回線が細いと、その移動そのものがジョブのコストになります。

AetherMesh は既定を反転させます。

```
データを計算機へ送る
        ↓  そのほうが安いときだけ
計算機をデータの近くへ送る
```

ここから 4 つの柱が出てきます。

| 柱 | 中身 |
|---|---|
| **Compute Optimization** | ラウンドロビンではなくスコアでノードを選ぶ |
| **Data Locality** | どのノードがどのデータを持っているかを把握する |
| **Transfer Optimization** | content addressing・chunk 単位の重複排除・適応的圧縮 |
| **Distributed Runtime** | 登録・heartbeat・配送・再試行・結果回収 |

---

## 何が違うのか

### データは一度しか流れない

すべてのデータセットは **BLAKE3 ハッシュ** で識別されます。コントローラはどのノードが何を持っているかを追跡するので、100 個のタスクが読む 8 MiB のデータでも転送は **1 回** です。大きなデータは content-addressed な chunk に分割され、受信側が既に持っている chunk（同じデータセット内の重複でも、別データセット由来でも）は二度と送られません。

### 圧縮は「判断」であって反射ではない

4 KiB 未満は素通し。約 800 Mbps より速いリンクでは圧縮しません（バイトより CPU のほうが高いため）。LZ4 の結果は **5% 以上縮んだときだけ** 採用します。乱数データを無駄に圧縮器へ通しません。

### 読めて調整できるスケジューラ

```
score = compute_cost + transfer_cost + latency_penalty − locality_bonus
```

小さいほうが勝ち。係数はすべて設定可能で、スコアは項目別に返るので **なぜそのノードが選ばれたか** が分かります。`LeastLoadedScheduler` / `LocalityScheduler` / `AdvancedScheduler` の 3 種類を同梱。

レイテンシと帯域は**実測**します。コントローラが小さい ping と大きい ping を送り、その差から往復遅延とスループットを推定して、指数移動平均でスコアに反映します。

### 障害は日常

heartbeat が途切れたノードは退去させ、そのノードが持っていたデータの所在も忘れます。配送に失敗したタスクは次に良いノードへ再配置され、必要なデータも一緒に運ばれます。**実行されて失敗したタスク**は結果として返り、無限に再試行はしません。

### 他言語の処理を、他人のマシンで

タスクは **WebAssembly** として動きます。処理は TypeScript・Rust・Go・C など WASM に出せる言語で書けて、それでもノードは非サンドボックスのプロセスを一切起動しません。モジュールに与えられるのはメモリと入力バッファと fuel だけ。ファイルもネットワークも時計もありません。

```ts
const module = await mesh.publishFile("uppercase.wasm");
const result = await mesh.runWasm(module.dataId, new TextEncoder().encode("hello"));
new TextDecoder().decode(result.output); // "HELLO"
```

宣言した入力データセットだけは、モジュールが明示的に要求したときに読めます（`aether.input_count` / `input_len` / `input_read`）。モジュール自体も通常のデータセットとして publish されるので、5 MB のモジュールでも各ノードへ 1 回だけ転送され、locality の加点対象になります。無限ループのコストは「1 タスク」であって「1 ノード」ではありません。

詳細と言語別のビルド手順: [`docs/wasm-tasks.md`](docs/wasm-tasks.md)

---

## 現状

**アルファ。ロードマップの全項目を実装済み、テスト 256 本。実運用実績はまだありません。**

実装済み: コア型・ワイヤプロトコル・ノードレジストリ・メトリクス収集・3 種のスケジューラ・TCP/TLS トランスポート（相互 TLS 対応）・トークン認証（メッシュ共通 + ノード別）・永続ノード ID・組込タスクと WASM タスク（ホスト機能は opt-in）・content addressing と chunk 転送と重複排除・**複数コネクションによる並列転送**・適応的圧縮・再試行と heartbeat 退去・レイテンシ/帯域の実測・JSON クライアント API（TLS 対応）・TypeScript / Python / Go の SDK・TOML 設定・カウンタと構造化ログ・**Kubernetes / AWS / GCP / Azure / ローカルプロセスのクラウドアダプタ**・ベンチマーク一式（Dask 比較を含む）。

正直な限界 — 「未実装」ではなく「未検証」の項目です:

- **クラウドアダプタは HTTP コンタクトのみ検証済み**です。各アダプタはスタブサーバに対して「送信するリクエスト」と「解析するレスポンス」を検証していますが、実際の AWS / GKE アカウントに対しては実行していません。実運用では実アカウント固有の細部に遭遇するはずです。
- **WASM のホスト機能は既定で無効**です。宣言済みデータセットの読み出しは常に可能、時計・乱数・ログは運用者が明示的に許可した場合のみ。ファイルとネットワークは今後も与えません。
- **パーセンタイルは loopback 計測**です。Dask 比較も内部ベンチも同一マシン上の数値です。
- **セキュリティレビューは未実施**です。相互 TLS・定数時間比較・ホストアクセスなしのサンドボックスは「設計」であり、レビューではありません。

---

## クイックスタート

Rust 1.85 以上（edition 2024）。

```bash
git clone https://github.com/syuumaimikan/AetherMesh && cd AetherMesh && cargo build --release
```

コントローラを起動:

```bash
cargo run -p aether-controller -- --listen 127.0.0.1:7000
```

ノードを参加させる（別のマシンでも同じ）:

```bash
cargo run -p aether-agent -- --controller 127.0.0.1:7000 --heartbeat-secs 5
```

Node から仕事を投げる（Node 22.6 以降なら .ts を直接実行できます）:

```bash
cargo run -p aether-wasm --example wat2wasm -- examples/wasm/uppercase.wat uppercase.wasm
node sdk/typescript/examples/wasm.ts uppercase.wasm "hello from typescript"
```

```
module 58e46fef2361318a… (233 bytes)
output: HELLO FROM TYPESCRIPT
ran on aebf4c04 in 2.02 ms
```

Python から:

```python
import sys; sys.path.insert(0, "sdk/python")
from aethermesh import AetherMesh

with AetherMesh.connect(port=7100) as mesh:
    data = mesh.publish(open("input.bin", "rb").read())      # 転送は 1 回だけ
    result = mesh.run("hash", b"seed", inputs=[data.data_id])
    print(result.output.hex(), f"{result.duration_ms:.1f} ms")
```

3 台構成（PC / Raspberry Pi / クラウド VM）の手順: [`docs/multi-node.md`](docs/multi-node.md)

---

## LAN の外に出す前に

TLS と認証は `tls` feature の裏にあります。

```bash
cargo run -p aether-controller --features tls -- generate-cert --host mesh.example.com
cargo run -p aether-controller --features tls -- --config controller.toml
cargo run -p aether-agent --features tls -- --controller mesh.example.com:7000 --tls-ca cert.pem
```

```toml
# controller.toml
listen = "0.0.0.0:7000"
client_listen = "0.0.0.0:7100"
auth_token = "change-me"          # メッシュ共通トークン
tls_cert_path = "cert.pem"
tls_key_path = "key.pem"
heartbeat_timeout_secs = 30
probe_interval_secs = 60          # レイテンシ・帯域の実測間隔

[node_tokens]                     # ノード別トークン（1台だけ失効させられる）
rpi4 = "token-for-the-pi"
desktop = "token-for-the-desktop"
```

トークンは `AETHERMESH_TOKEN` 環境変数からも渡せます。トークンの比較は定数時間で行い、どのトークンに近かったかは応答からも所要時間からも分かりません。TLS を有効にすると、エージェント側とクライアント API の**両方**が TLS になります。

設定ファイルの全項目: [`examples/controller.toml`](examples/controller.toml) / [`examples/agent.toml`](examples/agent.toml)

---

## ベンチマーク

### 既存システム（Dask）との比較

[Dask distributed](https://distributed.dask.org) は比較対象として最も近い実在システムです（スケジューラ・ワーカー・データを伴うタスク）。

```bash
python -m pip install "dask[distributed]"
cargo build --release -p aether-controller -p aether-agent
python bench/comparison/compare.py --tasks 100 --workers 3
```

100 タスク / 3 ワーカー / 同一マシン（16 コア Windows）/ loopback:

| system | workload | tasks/s | wall ms | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| **aethermesh** | overhead | **5,503** | 18 | **0.17** | **0.26** |
| dask | overhead | 63 | 1,582 | 15.39 | 39.09 |
| **aethermesh** | dataset (8 MiB) | **402** | 249 | **1.67** | **2.47** |
| dask-scatter | dataset (8 MiB) | 31 | 3,232 | 30.86 | 46.18 |
| dask-naive | dataset (8 MiB) | 21 | 4,699 | 40.39 | 87.56 |

`dask-naive` はデータセットをタスクにキャプチャする書き方（毎回コピーが飛ぶ）、`dask-scatter` は `client.scatter(..., broadcast=True)` を使う定石です。**AetherMesh は、利用者が意識しなくても scatter 相当の挙動になります。**

**この比較が示していないこと**（重要）:

- **タスク本体は同一ではありません。** Dask は Python の `hashlib.blake2b`、AetherMesh は Rust の BLAKE3 組込タスクを実行します。したがって dataset 行にはフレームワーク差と言語・アルゴリズム差が混ざっています。**フレームワーク同士の公平な比較は overhead 行**（何もしないタスク）です。
- **機能比較ではありません。** Dask のほうが遥かに多機能です（任意の Python 関数、依存関係のあるタスクグラフ、DataFrame/Array、スピル、オートスケール、ダッシュボード）。AetherMesh が実行できるのは組込タスクと WASM モジュールだけです。
- **ネットワーク測定ではありません。** 同一マシンの loopback です。実回線ではデータ移動の差はもっと開き（8 MiB を毎回送るコストは 100 Mbps では桁違い）、オーバーヘッドの差は縮みます。
- **スケーリング調査ではありません。** クライアント 1・逐次投入・ワーカー 3 です。

ハーネスをリポジトリに含めてあるのは、数字ではなく**再現手段**を渡すためです。

### 自分自身との比較（最適化レイヤの効果）

```bash
cargo run -p aether-benchmark -- compare --tasks 100 --nodes 3 --dataset-bytes 8388608
```

| 指標 | Baseline | AetherMesh |
|---|---:|---:|
| 転送バイト数 | 839,291,600 | **477,569** |
| トラフィック削減 | — | **99.9 %** |
| 実行時間 | 71,173 ms | **404 ms** |
| P50 / P95 / P99 | 708 / 743 / 750 ms | **2.8 / 2.9 / 3.0 ms** |

Baseline は「重複排除なし・chunk なし・圧縮なし・負荷のみのスケジューリング」= 素朴なディスパッチャの挙動です。両者とも in-process で実際の codec と executor を通しているので、この数字は**最適化レイヤの効果**であって実ネットワーク性能ではありません。

詳細と読み方: [`docs/benchmarks.md`](docs/benchmarks.md)

---

## アーキテクチャ

| Crate | 役割 |
|---|---|
| `aether-core` | 共有型: ID・ノード・タスク・データ記述子・ストア・chunk 化・圧縮。I/O なし |
| `aether-protocol` | ワイヤメッセージ、bincode エンコード、長さ前置の非同期フレーミング |
| `aether-scheduler` | `Scheduler` trait、データカタログ、3 種の配置ポリシー |
| `aether-controller` | レジストリ・接続・配送・再試行・ヘルス・リンク実測・サーバ（TCP/TLS）・クライアント API |
| `aether-agent` | ワーカー: identity・登録・メトリクス・データストア・タスク実行 |
| `aether-wasm` | サンドボックス WASM 実行（既定 `wasmi`、`wasmtime` も選択可） |
| `aether-cloud` | `CloudProvider` 抽象: 資源探索・ワーカー配備・プラットフォーム側メトリクス |
| `aether-benchmark` | Baseline と AetherMesh の比較計測（JSON 出力） |
| `sdk/typescript` | 依存ゼロの Node クライアント |
| `sdk/python` | 依存ゼロの Python クライアント |
| `sdk/go` | 標準ライブラリのみの Go クライアント（未コンパイル検証） |

### クライアントプロトコル

コントローラはエージェント用とは別に、クライアント用のリスナーを持ちます。形式は「4 バイトのビッグエンディアン長 + JSON オブジェクト 1 個」を双方向に流すだけです。

```json
{"type":"submit","kind":"wasm","module":"58e46f…","payload":"aGVsbG8="}
{"type":"result","success":true,"output":"SEVMTE8=","node_id":"aebf4c04…","duration_ms":2.02}
```

メッセージ種別は `hello` / `publish` / `submit` / `nodes` とその応答だけ。どの言語でも 200 行程度で実装できます（実際、同梱の 3 つの SDK がそれぞれその程度です）。

---

## 設計原則

```
Correctness → Simplicity → Performance → Extensibility
```

- ワークスペース全体で `unsafe` を禁止
- テスト以外で `unwrap()` を使わない。失敗はすべて `thiserror` 型
- 依存はフェーズが必要としたときにだけ追加する。全体で tokio・serde・bincode・blake3・lz4_flex・sysinfo・clap・tracing・toml・base64・wasmi、`tls` 有効時のみ rustls、JIT を選んだときのみ wasmtime
- 暗号と圧縮は pure Rust。Raspberry Pi 向けのクロスビルドに C ツールチェーンが要らない
- 各フェーズは必ず green で終える

```bash
cargo test --workspace
cargo test --workspace --features aether-controller/tls,aether-agent/tls
cargo test -p aether-wasm --no-default-features --features wasmtime-backend
```

---

## これから

- クラウドアダプタを実アカウントで動かし、そこで判明する差分を潰す
- QUIC トランスポート（フレーミング層は既に transport 非依存）
- サンドボックスと認証経路のセキュリティレビュー
- コストをスコアの項に加えたリージョン横断スケジューリング

## 追加した機能の使い方

### 相互 TLS

```bash
# CA と、それに署名されたサーバ証明書を作る
aether-controller generate-cert --with-ca --host mesh.example.com

# ノードごとにクライアント証明書を発行する（1台だけ失効させられる）
aether-controller issue-client-cert --name rpi4 \
  --cert-path rpi4.pem --key-path rpi4.key
```

```toml
# controller.toml
tls_cert_path = "cert.pem"
tls_key_path = "key.pem"
tls_client_ca_path = "ca.pem"   # これを設定すると相互 TLS になる
```

```bash
aether-agent --controller mesh.example.com:7000 --tls-ca ca.pem \
  --tls-client-cert rpi4.pem --tls-client-key rpi4.key
```

証明書を持たないエージェントは、トークンを提示する前の TLS ハンドシェイク段階で拒否されます。

### 並列転送

```bash
aether-agent --controller mesh.example.com:7000 --data-channels 4
```

エージェントが追加のコネクションを提供すると、コントローラは chunk をそれらに分散送信します。chunk は自己記述（data_id + index）なので順序保証は不要ですが、タスクとの順序関係も失われるため、**エージェントがデータセットの再構成完了を通知し、コントローラはそれを待ってからタスクを配送**します。

### WASM のホスト機能

既定では宣言済みデータセットの読み出しのみ。運用者が許可した場合に限り以下が使えます。

| import | 意味 |
|---|---|
| `aether.log(ptr, len)` | ノードのログに 1 行書く |
| `aether.now_unix_millis()` | 壁時計を読む（サイドチャネルにもなる） |
| `aether.random(ptr, len)` | 乱数を得る（タスクが非決定的になる） |

許可されていない import を持つモジュールは、こっそりスタブに繋がるのではなく **インスタンス化に失敗** します。

### クラウドアダプタ

```rust
// Kubernetes: Pod 内なら設定不要（service account トークンと CA を自動で読む）
let provider = KubernetesProvider::in_cluster("aethermesh", "ghcr.io/example/aether-agent:latest")?;
for resource in provider.discover_resources().await? {
    provider.deploy_worker(&resource.id, &WorkerSpec::new("mesh.example.com:7000")).await?;
}
```

AWS は SigV4 を自前実装（SDK 依存なし）、GCP と Azure はメタデータサーバのトークンを使います。`cloud-http` feature が必要です。

Issue と Pull Request を歓迎します。変更は小さく、テストを添えて、以下を通してから送ってください。

```bash
cargo fmt --all --check && cargo test --workspace
```

---

## ライセンス

Apache License 2.0 または MIT の**デュアルライセンス**。どちらか選択して利用できます。
