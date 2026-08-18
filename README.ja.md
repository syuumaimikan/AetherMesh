<img src="assets/logo.svg" width="88" align="right" alt="">

# AetherMesh

**データを運ぶのではなく、処理のほうを動かす。**

AetherMesh は、いま動かしている環境 — クラウドの VM、VPS、机の下の PC、棚の上の Raspberry Pi — の上に重ねて使う Rust 製のレイヤーです。やることは 2 つだけ。

- **どのノードで処理を走らせるか**を、負荷・レイテンシ・帯域・データの置き場所から決める
- **そのために何バイト動かす必要があるか**を最小にする

処理そのものは WebAssembly として実行できるので、TypeScript でも Rust でも Go でも書けます。投げ込む側は Node / Python / Go の SDK から。

英語版: [README.md](README.md) ／ ドキュメントサイト: [syuumaimikan.github.io/AetherMesh](https://syuumaimikan.github.io/AetherMesh/)

---

## 何が問題なのか

ふつうの分散処理は、**コードのある場所へデータを運びます**。データが大きくて回線が細いと、その転送時間がジョブの実行時間のほぼ全部になります。8 MiB のデータを 100 個のタスクが読むなら、素朴に書けば 800 MiB が流れます。

AetherMesh は既定を逆にします。

```
データを計算機へ運ぶ
        ↓  そのほうが安いときだけ
計算機をデータのそばへ持っていく
```

ここから 4 本の柱が出てきます。

| 柱 | 中身 |
|---|---|
| 計算配置の最適化 | 順番に割り当てるのではなく、スコアでノードを選ぶ |
| データ局所性 | どのノードが何を持っているかを把握し続ける |
| 転送の最適化 | content addressing、chunk 単位の重複排除、適応的圧縮 |
| 分散ランタイム | 登録・heartbeat・配送・再試行・結果回収 |

---

## 特徴

### データは一度しか流れない

データセットは **BLAKE3 ハッシュ**で識別します。同じ内容なら同じ名前になるので、100 個のタスクが読む 8 MiB は **1 回**しか転送されません。大きなデータは chunk に分割され、受け取る側がすでに持っている chunk は — 同じデータセット内の重複でも、別のデータセット由来でも — 二度と送られません。

### 圧縮するかどうかは、その場で判断する

4 KiB 未満は素通し。800 Mbps より速いリンクでは圧縮しません（バイトを削るより CPU のほうが高くつくため）。LZ4 をかけて **5% 以上縮まなかったら**、圧縮結果は捨てて生データを送ります。乱数データを無駄に圧縮器へ通したりしません。

### スケジューラは読めるし、調整できる

```
スコア = 計算コスト + 転送コスト + レイテンシ − 局所性ボーナス
```

小さいほうが勝ち。係数はすべて設定でき、スコアは項目ごとに返ってくるので、**なぜそのノードが選ばれたのか**が分かります。`LeastLoaded` / `Locality` / `Advanced` の 3 種類を同梱。

レイテンシと帯域は推測ではなく**実測**します。コントローラが小さい ping と大きい ping を打ち、その差から往復遅延とスループットを求めて、指数移動平均でスコアに反映します。

### どのマシンでもいい、とは限らない

コストが決めるのは「どこが**安いか**」で、「どこで**動かしていいか**」は別の話です。エージェント側は自分が何者かを名乗り、タスク側は必要な条件を書きます。

```bash
aether-agent --label gpu=true --label region=eu-west
```

```python
mesh.run("hash", payload, constraints=["gpu=true", "region!=us-east"])
```

書き方は `key=value`、`key!=value`、それと「そのラベルがあること」だけを見る `key` の 3 つ。これは優先度ではなく**足切り**です。条件を満たすノードが 1 台もなければ、どこかに妥協して投げるのではなく、そのことをエラーとして返します。GPU 前提の処理を CPU 機に流さない、規制のあるデータを管轄外に出さない、という用途を想定しています。

### どれだけ抱えるかを、ノード側で決められる

受け取ったデータセットはキャッシュします。次に同じデータを読むタスクが来たとき、もう一度運ばずに済むからです。ただし何もしなければ、このキャッシュはエージェントが動いている限り増え続けます。ワークステーションなら耐えますが、RAM 1 GB のボードでは致命的です。

```bash
aether-agent --storage-budget-mb 256
```

上限を超えると、使われていないものから捨てます。そして **何を捨てたかをコントローラに伝えます**。ここが肝心で、捨てたデータを「まだ持っている」とカタログが信じたままだと、入力がもう無いノードに仕事が送られ続けます。実行中のタスクが掴んでいるデータは、そのタスクが終わるまで生き残ります。

### 急ぎの仕事は、行列の後ろで待たない

どこで動かすかは配置が決め、いつ動かすかは優先度キューが決めます。5 段階と、それぞれ一文で説明できる 3 つの規則だけです。

```python
mesh.run("cpu", payload, priority="critical")     # critical · high · normal · low · background
```

優先度が高いものが先。同じ優先度なら到着順。**そして待ち時間が効きます** — 30 秒待つごとに 1 段階昇格するので、critical が流れ続けても background は「遅くなる」だけで「永久に走らない」にはなりません。この 3 番目の規則が無いと、最低優先度は優先度ではなく、果たされない約束になります。

1 ノードに background 400 本を同時投入した実測: キューは最大 381 まで積み上がり、0.25 秒後に投入した critical は 401 本中 57 番目で完了しました。まだ待っていた約 340 本の前に出て、すでに走り終えていた約 56 本の後ろ、という意味です。

### キューが溢れたときの挙動は、事故ではなく選択

既定では無制限です。黙って仕事を断り始めるメッシュは、目に見えて遅れるメッシュより厄介だからです。上限を設けるときは、何を捨てるかを選びます。

```toml
max_queue_size = 32
queue_rejection = "reject"        # drop_oldest / drop_lowest_priority も可
queue_timeout_secs = 30
```

`reject` は新しいものを断ります。返ってきた submit は再試行も報告もできるので、多くの場合これが欲しい挙動です。`drop_oldest` はライブフィード向け — 古い仕事はラベルが何であれ価値がない、という判断。`drop_lowest_priority` は満杯のキューを重要な仕事で埋めたままにし、自分が最下位なら断ります。

呼び出し側が自分の期限を持つこともできます。submit の `timeout_ms` は「この仕事は何ミリ秒待つ価値があるか」で、他の誰の期限も変えません。

どの場合でも、呼び出し側には必ず結果が返ります。1 ノードに 300 本同時投入した実測:

| | 実行された | 拒否 | 期限切れ |
|---|---|---|---|
| `max_queue_size = 32` | 42 | **258** | 0 |
| 上限なし・`timeout_ms = 200` | 45 | 0 | **255** |

返ってこない reply channel を掴んだままの submit は 1 本もありません。ここが肝心です。

### 遊んでいるノードは、ほとんど電気を食わない

メッシュは大半の時間、何もしていません。heartbeat の間隔が固定だと、その「何もしていない状態」が一番高くつきます。数秒おきに全マシンのコアが起きて、「変化なし」とだけ言うためです。

そこで間隔は固定にしていません。タスクを走らせているノード、あるいは負荷が目に見えて動いたノード（メッシュ以外の処理でも構いません）は設定どおりの間隔で報告します。何も起きていないノードは、報告するたびに間隔を倍にしていきます。

上限を決めるのはエージェントではなくコントローラです。登録時に「何秒沈黙したら退去させるか」を伝え、エージェントはその半分までしか間隔を伸ばしません。heartbeat が 1 回落ちただけで健全なノードが退去させられることはない、という余裕です。`heartbeat_timeout_secs` を大きくすれば、遊んでいるノードは勝手に静かになります。

### 落ちても止まらない

heartbeat が途切れたノードは退去させ、そのノードが持っていたデータの記録も消します。配送に失敗したタスクは次に良いノードへ回し、必要なデータも一緒に運びます。**走った結果として失敗したタスク**は結果として返し、無限に再試行はしません。

### 他人のコードを、安全に走らせる

タスクは **WebAssembly** として動きます。モジュールに渡すのはメモリと入力バッファと fuel（命令数の予算）だけ。ファイルもネットワークも時計もありません。

```ts
const module = await mesh.publishFile("uppercase.wasm");
const result = await mesh.runWasm(module.dataId, new TextEncoder().encode("hello"));
new TextDecoder().decode(result.output); // "HELLO"
```

モジュール自体も普通のデータセットとして扱われるので、5 MB のモジュールが 100 タスクで使われても転送は 1 回。無限ループのコストは「1 タスク」であって「1 ノード」ではありません。

言語別の書き方: [`docs/wasm-tasks.md`](docs/wasm-tasks.md)

---

## 動かしてみる

Rust 1.88 以上。Windows / macOS / Linux / Raspberry Pi、どれも同じように動きます。

```bash
git clone https://github.com/syuumaimikan/AetherMesh
cd AetherMesh
cargo build --release
```

**1. コントローラを起動**（エージェント用 7000 番、クライアント用 7100 番）

```bash
cargo run --release -p aether-controller
```

**2. ノードを参加させる**（別のマシンでも同じコマンド）

```bash
cargo run --release -p aether-agent -- --controller 192.168.1.10:7000
```

**3. 仕事を投げる**

```python
from aethermesh import AetherMesh

with AetherMesh.connect(port=7100) as mesh:
    data = mesh.publish(open("input.bin", "rb").read())   # 転送されるのは 1 回だけ
    for i in range(24):
        print(mesh.run("hash", str(i).encode(), inputs=[data.data_id]).node_id)
```

Node 22.6 以降なら TypeScript をそのまま実行できます。

```bash
cargo run -p aether-wasm --example wat2wasm -- examples/wasm/uppercase.wat uppercase.wasm
node sdk/typescript/examples/wasm.ts uppercase.wasm "hello from typescript"
```

```
module 58e46fef2361318a… (233 bytes)
output: HELLO FROM TYPESCRIPT
ran on aebf4c04 in 2.02 ms
```

### サンプル集

[`examples/`](examples) に、実際に動かせるものが順番に並んでいます。

| | 内容 |
|---|---|
| [01](examples/01-one-terminal) | ターミナル 1 枚でメッシュ全部 |
| [02](examples/02-two-terminals) | コントローラとエージェントを別プロセスに |
| [03](examples/03-many-agents) | 1 台の中でエージェント複数、配置が変わる様子 |
| [04](examples/04-two-devices) | ノート PC と Raspberry Pi を 1 つのメッシュに |
| [05](examples/05-web-app) | ブラウザからメッシュを使う（ブリッジ経由） |
| [06](examples/06-python-pipeline) | 1 回 publish して何度も回す |
| [07](examples/07-wasm-task) | 他の言語で書いたタスク |
| [08](examples/08-secure-mesh) | TLS・トークン・クライアント証明書 |

---

## LAN の外に出す前に

ここまでの手順はすべて認証なし・localhost 前提です。実ネットワークに出す前に TLS とトークンを有効にしてください。

```bash
# CA と、それに署名されたサーバ証明書を作る
cargo run --release -p aether-controller --features tls -- generate-cert --with-ca --host mesh.example.com

# ノードごとにクライアント証明書を発行する（1 台だけ失効させられる）
cargo run --release -p aether-controller --features tls -- issue-client-cert --name rpi4
```

```toml
# controller.toml
listen        = "0.0.0.0:7000"
client_listen = "0.0.0.0:7100"

tls_cert_path      = "cert.pem"
tls_key_path       = "key.pem"
tls_client_ca_path = "ca.pem"     # ← この 1 行で相互 TLS になる

[node_tokens]                     # ノードごとのトークン
rpi4    = "…"
desktop = "…"
```

証明書を持たないエージェントは、トークンを出す前の TLS ハンドシェイクの段階で弾かれます。トークンの比較は定数時間で、どのトークンに近かったかは応答からも所要時間からも分かりません。

脅威モデルと「守らないと決めたもの」: [`docs/security.md`](docs/security.md)

---

## 動いているところを見る

```bash
cargo install --path crates/aether-tui
aether-tui --controller 127.0.0.1:7100
```

```
 AetherMesh ● live  127.0.0.1:7100  every 1.00s
┌ Throughput ────────────────────────────┐┌ Not moved ─────────────────────────┐┌ Mesh ──────────────────────────────┐
│7.3 MiB/s   peak 7.3 MiB/s              ││compressed away   1020.0 KiB        ││nodes             2/2 connected     │
│18.0 MiB on the wire so far             ││ratio             0.948             ││datasets          26 · 41.0 MiB     │
│ █                                      ││transfers skipped 2                 ││tasks ok          9                 │
│ █                                      ││chunks skipped    3                 ││tasks failed      0                 │
└────────────────────────────────────────┘└────────────────────────────────────┘└────────────────────────────────────┘
┌ Nodes (2) ─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│   host                     id        cpu    mem    rtt       link         holds            labels                  │
│●  syuum                    73ef9fd1  24%    80%    —         —            2 · 5.0 MiB      kind=gpu                │
│●  syuum                    824d28e0  24%    80%    —         —            —                kind=cpu region=eu-west │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 q quit   s send a task   ↑↓ node   +/- poll rate   r refresh   ? help
```

モックではなく、実際に動いているメッシュの 1 フレームです。

- **Not moved** — 送らずに済んだ量。圧縮で減った分、ノードが既に持っていて丸ごと省略できたデータセット数、重複排除で送らなかったチャンク数。ここが大きいほどメッシュが仕事をしています
- **holds** — そのノードが既に持っているデータセット。これを読むタスクは転送ゼロで走ります。スケジューラが毎回判断しているのはこの列です
- `○` のノードは登録済みだが到達不能。heartbeat がタイムアウトするまで登録は残します（1 回遅れただけで死んだ扱いにはしません）

`s` でタスクを投げると、どのノードで走ったかがその場で出ます。詳細は [`crates/aether-tui`](crates/aether-tui)。

### スクレイプする場合


```bash
aether-controller --metrics-listen 127.0.0.1:9100
curl 127.0.0.1:9100/metrics
```

```
aethermesh_nodes_registered_total 1
aethermesh_heartbeats_total 1
aethermesh_tasks_completed_total 0
# TYPE aethermesh_nodes gauge
aethermesh_nodes 1
aethermesh_nodes_connected 1
aethermesh_cpu_usage_mean 0.007223892025649548
```

`/metrics` が Prometheus 形式、`/healthz` が死活確認です。指定しない限り起動しません。

出すのはカウンタと平均値だけで、ホスト名・ノード ID・アドレスは含めていません。認証のないポートが、そのままネットワークの一覧表になってしまうからです。それでも localhost か管理用インタフェースに bind してください。

---

## ベンチマーク

### 既存システム（Dask）との比較

100 タスク / ワーカー 3 / 同一マシン（16 コア）/ loopback。

| システム | 内容 | tasks/s | p50 | p99 |
|---|---|---:|---:|---:|
| **AetherMesh** | フレームワークのオーバーヘッド | **5,503** | **0.17 ms** | **0.26 ms** |
| Dask | 同上 | 63 | 15.4 ms | 39.1 ms |
| **AetherMesh** | 8 MiB を共有する処理 | **402** | **1.67 ms** | **2.47 ms** |
| Dask + scatter | 同上 | 31 | 30.9 ms | 46.2 ms |
| Dask（素朴な書き方） | 同上 | 21 | 40.4 ms | 87.6 ms |

`dask-naive` はデータセットをタスクに閉じ込める書き方（毎回コピーが飛ぶ）、`dask-scatter` は `client.scatter(..., broadcast=True)` を使う定石です。**AetherMesh は、利用者が意識しなくても scatter 相当の挙動になります。**

**この数字が示していないこと。** タスクの中身は同一ではありません（Dask は Python の `blake2b`、AetherMesh は Rust の BLAKE3 組込タスク）。したがってデータセット側の行にはフレームワーク差と言語差が混ざっています。**フレームワーク同士の公平な比較は overhead 行**です。また Dask のほうが遥かに多機能ですし、これは loopback であって実ネットワークではありません。

方法論と注意点の全文: [`docs/benchmarks.md`](docs/benchmarks.md)

### 素朴なディスパッチャとの比較

```bash
cargo run -p aether-benchmark -- compare --tasks 100 --nodes 3 --dataset-bytes 8388608
```

| 指標 | 最適化なし | AetherMesh |
|---|---:|---:|
| 転送バイト数 | 839,291,600 | **477,569** |
| 削減率 | — | **99.9 %** |
| 実行時間 | 71,173 ms | **404 ms** |
| P50 / P95 / P99 | 708 / 743 / 750 ms | **2.8 / 2.9 / 3.0 ms** |

同じバイナリの最適化を全部切った状態が「最適化なし」です。どちらも in-process で実際の codec と executor を通しているので、これは**最適化レイヤの効果**であって実ネットワーク性能ではありません。

---

## 構成

| Crate | 役割 |
|---|---|
| `aether-core` | 共有型: ID・ノード・タスク・データ記述子・ストア・chunk 化・圧縮。I/O なし |
| `aether-protocol` | ワイヤメッセージ、bincode エンコード、長さ前置の非同期フレーミング |
| `aether-scheduler` | `Scheduler` trait、データカタログ、3 種の配置ポリシー |
| `aether-controller` | レジストリ・接続・配送・再試行・ヘルス・リンク実測・サーバ・クライアント API |
| `aether-agent` | ワーカー: identity・登録・メトリクス・データストア・タスク実行 |
| `aether-wasm` | サンドボックス WASM 実行（既定 `wasmi`、`wasmtime` も選択可） |
| `aether-cloud` | Kubernetes / AWS / GCP / Azure / ローカルプロセスのアダプタ |
| `aether-benchmark` | 最適化なしとの比較計測（JSON 出力） |
| `sdk/{typescript,python,go,java,dotnet}` | 依存ゼロのクライアント |

### クライアントプロトコル

コントローラはエージェント用とは別に、クライアント用のリスナーを持ちます。形式は「4 バイトのビッグエンディアン長 + JSON オブジェクト 1 個」を双方向に流すだけです。

```json
{"type":"submit","kind":"wasm","module":"58e46f…","payload":"aGVsbG8="}
{"type":"result","success":true,"output":"SEVMTE8=","node_id":"aebf4c04…","duration_ms":2.02}
```

メッセージ種別は `hello` / `publish` / `submit` / `nodes` とその応答だけなので、どの言語でも 200 行ほどで実装できます。同梱の SDK は、実際どれもそれくらいの分量です。

| SDK | 必要なもの | 備考 |
|---|---|---|
| [TypeScript / JavaScript](sdk/typescript) | Node 20+ | 参照実装 |
| [Python](sdk/python) | 3.10+ | `concurrent.futures` 互換の `MeshExecutor` つき |
| [Go](sdk/go) | 1.21+ | 標準ライブラリのみ |
| [Java](sdk/java) | 17+ | 2 ファイル。ビルドツール不要 |
| [C# / .NET](sdk/dotnet) | 8+ | 全面 `async`、`IAsyncDisposable` |

JSON ライブラリすら使っていません（標準にない言語では自前で書いています）。目的の言語が無ければ、上のプロトコルが仕様のすべてです。

---

## 設計の考え方

```
正しさ → 単純さ → 速さ → 拡張性
```

- ワークスペース全体で `unsafe` 禁止
- テスト以外で `unwrap()` を使わない。失敗はすべて `thiserror` 型
- 依存は必要になったときにだけ足す。既定の依存は tokio・serde・bincode・blake3・lz4_flex・sysinfo・clap・tracing・toml・base64・wasmi のみ。TLS もクラウドアダプタも JIT も opt-in
- 暗号と圧縮は pure Rust。Raspberry Pi 向けのクロスビルドに C ツールチェーンが要らない
- リリースビルドは小ささ優先（LTO・シンボル削除・panic=abort）。コントローラ 1.3 MB、エージェント 2.7 MB

```bash
cargo test --workspace
cargo test --workspace --features aether-controller/tls,aether-agent/tls,aether-cloud/cloud-http
cargo test -p aether-wasm --no-default-features --features wasmtime-backend
```

---

## 現状と、正直な限界

**アルファ。ロードマップの項目はすべて実装済み（テスト 365 本）ですが、実運用実績はまだありません。**

- **クラウドアダプタは HTTP のやり取りだけを検証しています。** 送信するリクエストと解析するレスポンスをスタブサーバで確認済みですが、実際の AWS や GKE のアカウントに対しては実行していません。
- **WASM のホスト機能は既定で無効です。** 宣言したデータセットの読み出しは常に可能。時計・乱数・ログ・特定ディレクトリの読み取りは、運用者が明示的に許可したときだけ使えます。ファイルとネットワークは今後も与えません。
- **ベンチマークは loopback 計測です。** 実回線ではデータ移動の差はもっと開き、オーバーヘッドの差は縮みます。
- **ラベルは誰も検証しません。** GPU の無いマシンが `gpu=true` を名乗れば、GPU 前提の仕事が送られてきて失敗します。ラベルは配送の仕組みであって、ハードウェアの棚卸しではありません。
- **第三者によるセキュリティレビューは未実施です。** 相互 TLS・定数時間比較・ホストアクセスなしのサンドボックスは「設計」であり、レビューを受けたわけではありません。内部レビューでは実際に 3 件（データチャネルの乗っ取り、heartbeat の詐称、暗号用途に耐えない `random`）が見つかって直しました。4 件目が無いと考える理由はありません。

## これから

- クラウドアダプタを実アカウントで動かして、そこで判明する差分を潰す
- QUIC トランスポート（フレーミング層はすでに transport 非依存）
- サンドボックスと認証まわりのセキュリティレビュー
- コストをスコアの項に加えた、リージョンをまたぐ配置
- タスクの優先度とキューイング（今は先着順）
- 実ハードウェア・実回線での計測。この repository で一番弱いのはここです

Issue と Pull Request を歓迎します。変更は小さく、テストを添えて、以下を通してから送ってください。

```bash
cargo fmt --all --check && cargo test --workspace
```

## ライセンス

Apache License 2.0 または MIT のデュアルライセンス。どちらか好きなほうを選んで使えます。
