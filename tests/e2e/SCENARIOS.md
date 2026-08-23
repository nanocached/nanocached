# nanocached v0.2.0 ローカル検証シナリオ一覧

対象資産: `ghcr.io/nanocached/nanocached-node:0.2.0` / `nanocached-discovery:0.2.0`、PyPI `nanocached==0.2.0`。環境: Docker Desktop for Mac。
結果の詳細は `nanocached-e2e-report-v0.2.0.md` を参照。

## 第 1 ラウンド（実施済み・2026-08-22）

### A. 単一ノード

| # | シナリオ | 結果 |
|---|---|---|
| A1 | CRUD / TTL / pipeline 1000+1000 / tagged mode / バイナリ key・value | OK |
| A2 | 境界値: key 長 0、1 MiB 境界（header+body）、空 value、1 MiB key、不正ヘッダ 8 種 | OK（不正は切断、サーバ生存） |
| A3 | 接続上限: 同一 IP から 1,030 本、4 IP から 1,000+24 本、解放後の回復 | per-IP 256 上限が未記載（INFO）、グローバル 1,024 OK |
| A4 | idle timeout 60s（無通信 / 途中まで送信）、SDK keep-alive 95s | OK |
| A5 | `--max-memory 16MiB` で 200 MB 分書込 → LRU | OK（RSS 20 MiB） |
| A6 | 認証: 未認証コマンド / 誤 secret / 正 secret / tagged+auth | OK |
| A7 | TLS: 平文拒否 / 正 CA / 誤 CA / システム trust / TLS 越し 5000 set | OK |
| A8 | 200 接続 × 10 分 負荷（get70/set25/del5、整合性検証） | OK（26k ops/s、エラー 0） |

### B. クラスタ

| # | シナリオ | 結果 |
|---|---|---|
| B1/B2 | R=1 で 1→2→3→5→8 台へ逐次・同時追加、各段で全キー検証 | **BUG #62**（566/20000 消失、abandon 16 回） |
| B3 | R=2 で 1 台 kill → 即時 / eviction 後の読み取り | OK（bootstrap 失敗 WARN） |
| B3b | R=2 で 2 台 kill → 復旧 | 損失は仕様どおり、**BUG #61**（`W` エラー） |
| B4 | kill した node の再起動（新 identity）、負荷中の kill+restart | OK |
| B5 | `docker pause` 40s → eviction → unpause | OK |
| B6 | 負荷中の discovery 再起動（grace 中の `L`=B、新規クライアント） | OK |
| B7 | discovery HA 3 replica: primary kill / secondary kill / primary 停止中の join | OK |
| B7c | primary 復帰直後（grace 中）の join | **BUG #63**（1706/20000 迷子） |
| B8 | replica 間の `--replication-factor` 不一致 | OK（`L` を B で拒否） |
| B9 | 認証 + TLS クラスタ（node↔discovery 含む）、誤 secret / no TLS / `--tls-ca` なし | OK、IP SAN 必須（WARN）、誤ログ（INFO） |
| B10 | 180 MB 保持状態で node 追加（負荷中） | OK（7 秒） |
| B11 | join 中の負荷 | join 中 25% miss（WARN） |
| B11b | 負荷中の node kill（R=1） | **BUG #61** |
| B12a | 1 台に netem 200ms + 5% loss | 全体 76 ops/s に低下（WARN） |
| B12b | node→discovery のみ iptables 遮断 → 復旧 | OK |
| B12c | node→discovery 2s 遅延 | OK |

### C. ロングラン

| # | シナリオ | 結果 |
|---|---|---|
| C1 | 単一ノード定常 2h | リークなし（+0.09 MiB/h） |
| C2 | 単一ノード LRU 連続退避 2h（64 MiB） | リークなし（+0.07 MiB/h） |
| C3 | 接続チャーン 1h（50.7 万接続） | リークなし |
| C4 | TTL 3s 大量書込 1h | 失効分回収、リークなし |
| C5 | クラスタ 3 node + discovery 2 replica 定常 2h | リークなし（+0.05 MiB/h） |
| C6 | クラスタ churn 1h（kill/restart × 5） | リークなし、損失 0 |

## 第 2 ラウンド（追加シナリオ・費用対効果順、2026-08-22 実施）

| 順 | # | シナリオ | 目的 | 結果 |
|---|---|---|---|---|
| 1 | D1 | R=2 / R=3 で 1 台 kill 後の `get`/`set` を種別ごとに計測（`W` が replica leg に出るか、`replica_write_failures`） | #61 の影響範囲確定 | **#61 拡張**: R=2/3 でもクライアントエラーは出ないが、新規書込の 1/3（R=2: 1006/3000）〜1/2（R=3: 1476/3000）が 1 コピー不足。`replica_write_failures` に計上されるだけ |
| 2 | D2 | D1 と同じ障害を `read_repair=True` / `fire_and_forget_replicas=True` で実施 | SDK オプションが障害を隠すか直すか | read_repair / fire_and_forget を有効にしても R=2/3 の結果は同じ（隠しも直しもしない。R=1 の `W` は対象外） |
| 3 | D3 | node の graceful 停止（SIGTERM / `docker stop`）と kill の挙動差 | leave 処理の有無 | `docker stop` は exit 0 だが leave は伝わらず kill と同じ eviction 経路。bootstrap 失敗も同じ |
| 4 | D4 | Go / TypeScript / Rust SDK で #61 の障害と bootstrap 失敗（B3）を再現 | SDK 間の挙動差 | Python/Go/TS/Rust とも同一挙動（liveness 窓中の bootstrap 失敗、eviction 後 3332/20000 が WrongNode） |
| 5 | D5 | forwarding 窓終了 ±数秒での join（境界時間の特定） | #62 の最小再現・推奨待ち時間 | 境界は **60 秒ちょうど**（59s→abandon 2 回・67s、61s→0 回・1s）。全 Joined ノードが join ごとに新しい窓を持つ |
| 6 | D6 | #63 の R=1 版、および全 node 再 announce 後に迷子ノードを再起動して回収されるか | #63 の損失確定 | R=1 ではレジストリ空のため **ハンドオフなしで即 promote**、4965/20000 迷子。全員 announce 後に当該ノード再起動で 20000/20000 回復（回避策） |
| 7 | D7 | TTL 付きキーの join 移動で残り TTL が保持されるか | ハンドオフの TTL 正確性 | TTL 保持を確認（150s TTL、移動後も +140s で全件、+165s で 0 件） |
| 8 | D8 | `--max-memory` 到達中の join（joiner 側で即 LRU 退避） | 容量限界でのスケールアウト | 2×16MiB 満杯状態で 16MiB ノード追加: join 2 秒、保持件数不変、WARN なし |
| 9 | D9 | discovery replica 全滅 → 復旧 | HA の最悪ケース | replica 2 台同時 kill 40 秒（19.7k ops/s 負荷中）: エラー 0、復旧後 grace→members 3、join も正常 |
| 10 | D10 | TLS 有効クラスタで 1h 負荷 | TLS 周りのリーク | 1h・1,700 万 ops・TLS 再接続 1.7 万回でエラー 0、ノード RSS 25MiB 横ばい、discovery 1.4MiB。リークなし |
| 11 | D11 | abandon ループを起こし続ける 1h（discovery 側メモリ） | #62 時の discovery リーク | 35 サイクル・abandon 70 回で discovery RSS 0.65MiB・fd 12 一定、ノードも横ばい。リークなし |
| 12 | D12 | Linux ホスト（EC2 t3.small, AL2023）で A3 / B12a の再計測 | vpnkit の影響排除 | macOS と同一: per-IP 256 / グローバル 1,024、劣化ノード 1 台で 5,723→77 ops/s（p99 620ms）。Docker Desktop 起因ではない |
| 13 | D13 | B12 の切り分け: netem 条件（delay / loss / 両方）× 操作種別 × `fire_and_forget` | #64 の原因特定 | 遅延 200ms 単独で 21k→90 ops/s（get のみ 118、set のみ 59、faf 117）。loss 5% 単独は 1,709。遅延が主因で、遅いノードへのフェイルオーバーがない → **#64** |
| 14 | D14 | D13 中に発見: `fire_and_forget_replicas` + loss で `close()` がハング | SDK の終了処理 | `close()` の drain ループが完了済みタスクで busy loop 化しイベントループ全体が停止（deadline 期限超過でも未発火、`loop._ready` 固定）。修正案付きで **#65** に追記 |

## 第 3 ラウンド（v0.3.0 コードレビュー所見の再現、2026-08-23、公開済み 0.3.0 資産）

レビューで起票した #91〜#97 のうち「既存手法の延長で再現できそうなもの」を検証。資材（スクリプト実体）はこのディレクトリ（`tests/e2e/`、リポジトリ管理）。第 1・2 ラウンドの `scratchpad/e2e/` 記載は当時のセッション揮発パスの歴史的表記。

| # | シナリオ | 目的 | 結果 |
|---|---|---|---|
| E1 | アドレス churn 20 ラウンド（毎回**新しいポート**で node 追加 → kill → eviction → 削除、Python client 常駐、`i96.sh` + `churn96.py`） | #96 cooldown map の残留と client メモリ | **#96 再現**: `_redial_cooldowns` 0→20 件（白箱）、RSS +12 MB/8 分。tracemalloc で成長源は map ではなく **cooldown ヒットごとの `raise error` によるトレースバック肥大**（traceback 9.7k・frame 6.8k 残留、Python 固有）。注意: Docker は削除コンテナの IP を即再利用するため、ポートを変えないと同一キーで再現しない（C6 / v0.3.0 ロングランが拾えなかった理由） |
| E2 | ローカル完了後の join abandon（3 node R=1、b,c を `docker pause`、joiner d を kill → `X`、A の自キー 200 件を 100ms ごとに raw GET、`i93.sh` + `probe93.py`） | #93 `known_ring` 未復帰の窓 | **#93 再現 4/4**: `X` 直後に A が自キー 25〜58/200 件に `W`、次の heartbeat（5s 固定）で復帰、窓 0.05〜0.84s。副次: joiner の切断検知が M 配送完了まで遅れる（`J` 接続タスクが配送を await してから `wait_for_promotion` に入る） |
| — | #94（Joining 中の duplicate J） | — | コード確認で invalid（意図設計 #3/#9、ノードは旧接続エラー後にしか再 J しない）。クローズ |
| E3 | 偽 discovery（`fakedisc92.py`）が `H` に `A 5 1\n` + 改行なしバイト列を流し続ける、ノードは 256MB コンテナ上限・`--max-memory 32MiB`（`i92.sh`） | #92 heartbeat ack 無制限読み込み | **#92 再現**: ノードが **4 秒で OOMKilled**（exit 137）。偽 discovery は 255MiB 流した時点で切断。キャッシュのメモリ上限は `read_heartbeat_ack` の `read_until` をバイパスされ無効。対照の honest モード（`A\n`）ではノードは 1.73MiB で生存継続 |
| E4 | R=2・全ノード +40ms netem、`read_hedge_after=1ms`、40 並行 get() ループ中にランダムなタイミングで `close()`、close 直前 pending 数・close 後 leftover・teardown 後 add/IO を白箱計測（`i91.sh` + `probe91.py`、80 反復） | #91 hedge leg 登録 vs close() レース | **Python では再現せず**: 80/80 全反復で close() 時に hedge leg が pending（`active_close_iters=80`）だったが、leftover・teardown 後 add・teardown 後 IO すべて 0。`_drain_tasks` の `while tasks:` 再チェック＋leg0 登録が `_closed` チェックと同一同期バースト（間に yield 無し）＋ドレイン→`_teardown` 間に await 無し、で窓が塞がっている。Python（参照実装）では false positive の可能性。Rust（mutex 保持ドレイン + JoinSet）/TS/Java/.NET は drain 機構が異なり別途要検証 |
| — | #95（O(N²)）/ #97（Java close 5s） | — | 再現より修正＋ベンチ/テストが早いため未実施 |
