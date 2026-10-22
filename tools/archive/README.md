# Portable history archive

Build a private, ordinary SQLite database from an existing **decrypted snapshot**.
The source snapshot is retained; this tool does not log into WeChat, extract keys,
refresh a live snapshot, download missing attachments, or collect new messages.

Requires Python 3, SQLite with FTS5/trigram, and the system `libzstd` library.
No Python package installation is necessary.

```sh
python3 tools/archive/build_archive.py \
  --source /private/snapshot/databases \
  --output /private/structured/chat-archive.sqlite3 \
  --self-user YOUR_VERIFIED_WECHAT_USERNAME
python3 tools/archive/query_archive.py --db /private/structured/chat-archive.sqlite3 stats
python3 tools/archive/query_archive.py --db /private/structured/chat-archive.sqlite3 conversations --name '名称'
python3 tools/archive/query_archive.py --db /private/structured/chat-archive.sqlite3 history --chat USERNAME --limit 50
python3 tools/archive/query_archive.py --db /private/structured/chat-archive.sqlite3 search '关键词'
```

The output path must not exist. Build into a new path, validate the report, and
publish it as the current archive only after success. Snapshot directories and
outputs should be private (0700 directories, 0600 files). Query arguments and
results contain private data; do not publish them in logs or Git.

## Tables and interpretation

- `contacts`, `conversations`, `messages`: normalized identities and message fields.
  `chat_history` joins display names for convenient read-only SQL queries.
- `message_sources`: exact source database, table, and row ID, plus source payload
  hash. Read the original snapshot to recover all original columns and bytes.
  When a merged source contains different decoded text, `variant_text` retains it.
- `source_tables`: source/import counts for reconciliation.
- `index_evidence`: FTS content rows preserved separately. These may be snippets
  and are **not** added to the canonical message count.
- `auxiliary_messages`: chatbot source records, preserving binary fields as hex.
- `resource_messages`, `resource_details`: attachment metadata references. These
  do not assert that a referenced file exists or that an encoded image is decoded.
- `media_files`: optional index of transferred files; populate after transfer with
  `index_media.py --media /private/media --db /private/structured/chat-archive.sqlite3`.

Nonzero server IDs are merged only when conversation, timestamp, message
kind, and subtype also agree. Zero-ID rows retain their source identity, including
same-second messages. Source counts are distinct from canonical message counts.
Missing identities remain `unknown`/`unresolved`; outgoing direction is only set
when the sender matches the explicitly supplied, verified self username.

Text is decoded from UTF-8 and Zstandard. Failed decoding is explicit and the
source database retains its original bytes. Group sender prefixes are stripped
only when their shape/identity is recognizable. XML parsing extracts common
fields without resolving DTDs or external entities. Unknown message kinds remain
available as decoded text plus numeric type codes. Media bytes are not decoded.

Times remain Unix timestamps with a UTC ISO representation; convert at query/UI
boundaries. Search uses FTS5 trigram for at least three characters and escaped
`LIKE` for shorter queries (including short Chinese queries).

```sh
python3 -m unittest discover -s tools/archive -v
```

Acceptance checks include source-row reconciliation, foreign-key validation,
SQLite `quick_check`, FTS content/index integrity, and a report that separates
message counts, index evidence, decoding failures, and unresolved identities.

Use `search-index TEXT` to query the separately labeled index evidence. It is not
merged into the main history and may contain partial text or duplicate messages.

## Incremental Mac to NAS collection

`sync_incremental.py --state-dir PRIVATE_STATE --once` reads live encrypted
primary shards using the existing local SQLCipher library and keys. The private
state file identifies the previously verified plaintext snapshot and account.
Initial row cursors come from that snapshot; new shards start at row zero.
A recent row tail is replayed, so same-second messages are not skipped. A local
payload fingerprint avoids repeatedly uploading unchanged rows. NAS commits an
idempotent batch before the Mac advances its cursors. Changed source payloads
are retained in `incremental_edits`; arbitrary edits to older rows outside the
replay tail and deleted messages are not fully reconciled. Salt changes or rowid
resets stop collection explicitly. Keys and message payloads must never enter Git.
The deployment polls messages every minute while the Mac user session is active;
sleep/offline periods delay collection until a subsequent successful run.

`decode_media.py` validates decoded DAT images with Pillow before publishing a
separate output file. It takes image key material on stdin, and keeps a resumable
SQLite manifest. The output hash describes decoded bytes, not the encrypted input.
`convert_wxgf.py` converts a dominant HEVC partition through FFmpeg; ambiguous
multi-partition animations stay unresolved rather than being called complete.
This uses the partition layout documented in
[chatlog](https://github.com/sjzar/chatlog/blob/a16b689/pkg/util/dat2img/wxgf.go).

`link_media.py` writes a separate association database. A link requires an exact
MD5 reference in message XML plus the corresponding conversation directory.
`no_extractable_reference` is unknown coverage, not a missing-file assertion.
Original filenames, thumbnails, and full images are retained separately.
Media sync and indexing run independently from text; their refresh interval and
backlogs can make attachment availability later than message availability.

Additional media tests require `cryptography` and `Pillow` in an isolated venv.

For this deployment, the persistent supervisor calls `sync_incremental.cycle` and
`recent_media.main` every 60 seconds. The latter prioritizes up to 20 recent image
files per run, checks transfer hashes, and validates decoded output on NAS.
A separate 15-minute media sweep catches other attachments and older local files.
These are polling intervals, not guaranteed end-to-end latency. Backfills, sleep,
missing downloads, connection failures, or image formats needing further work
can delay availability. `attachments --message-id ID` on `query_archive.py`
returns association evidence and decoded/original paths once indexed. Voice
conversion and arbitrary older historical edits are outside these new workers.

### macOS privacy prompts: persistent application deployment

The earlier per-minute Python launch agents are retired. Directly relaunching a
command-line interpreter to open another application's container can repeatedly
trigger macOS cross-app privacy prompts. `macos/Launcher.swift` supplies a stable,
locally signed application identity, `WeChat NAS Sync.app`, which starts
`macos/supervisor.py` once. Three worker threads perform text, recent image, and
full-media work in that same Python process. The supervisor directly imports the media orchestrator and scanner from this
checkout. Each worker releases its file lock at the end of every round, including
failed rounds. SSH workers do not open the Mac's WeChat container.

The user must explicitly enable Full Disk Access for this dedicated app in System
Settings; do not edit TCC, grant broad access to a general Python interpreter, or
programmatically click a privacy consent button. Keep the app stopped until this
step is complete. Verify subsequent cycles under the app after authorization;
build/signature checks alone do not prove the authorization works.

Install one login agent that invokes `/usr/bin/open -gj APP_PATH` with RunAtLoad
and no StartInterval/KeepAlive. Remove the previous three plists from LaunchAgents,
not just unload them, so they cannot return at the next login. The app deliberately
does not automatically relaunch a crashing Python worker. Private local config
in `~/.wx-cli/nas-archive/supervisor-config.json` supplies the interpreter, repository,
and supervisor paths. Do not replace the app's executable/signature casually after
authorization. This application is not sandboxed: Full Disk Access is a broader
OS permission than the particular WeChat paths used by these scripts.


## Mac 排除名单与状态

仅在私有状态目录 `~/.wx-cli/nas-archive/exclude.json` 中保存名单，不加入 Git，
权限必须为 `0600`（目录为 `0700`）。下面均为示例占位值，实际 username 由人工填写：

```json
{"version": 1, "conversations": {"example_group@chatroom": "示例备注", "wxid_example": "示例备注"}}
```

每轮都会重新读取。不存在的文件表示空名单；JSON、版本、字段类型或权限不正确时，
这一轮报错且不上传。建议在同目录写好临时文件、`chmod 600` 后再 `mv` 替换名单。
名单和备注不会随同步包发送。

- 文字同步在读取消息表、检查会话解析和写游标之前跳过 `Msg_<md5(username)>`；
  资源引用按 `ChatName2Id` 过滤。近期图片也跳过排除会话的引用和附件目录。
- 全量媒体跳过整个 `msg/attach/<md5(username)>/`。视频和缩略图只有在其 token
  的全部已知引用均属于排除会话时才跳过；另一会话也引用的 token 会保留。
- `msg/file/YYYY-MM/` 原始文件按文件类 appmsg（type 6）的 `title` 和 `totallen`
  匹配文件名与字节数，并检查所有会话的引用。没有可靠归属时保留，记入
  `exclusion_unresolved`；存在非排除会话的已知引用时照常同步。仅当已知归属全部属于
  排除会话时，才逐条检查非排除会话的失败证据：可解码原文含该文件名（含 XML
  转义形式）时保留；没有文件名命中但存在无法解码的记录时保留并记入
  `exclusion_unresolved`；否则跳过。排除会话的失败证据不影响判断。
  NAS 清除使用同一个 `exclusion.py`，合并归档及所有快照的证据后判断；归档中的
  `incremental_edits` 也会解码并参与判断；新来源合并到已有消息时留下的
  `message_sources.variant_text` 同样参与文件和视频归属判断，避免快照及规范正文遗漏
  编辑或来源变体中的共享引用。正文、编辑和来源变体的解析失败均按上述逐文件规则
  判断；编辑证据损坏或缺字段导致原文不可用时，记录无法解码证据；无法确定会话时
  按非排除会话处理。

名单控制此后的上传，已有 NAS 数据需要另行清除。移除名单中的会话后，原来未写入的
文字游标可继续补传，媒体也会重新参与扫描。

三个 worker 的状态为 `incremental-status.json`、`recent-media-status.json`、
`media-cycle-status.json`，同时保留 supervisor 状态文件。每个状态包含 `stage`、
`last_success_at`、`consecutive_errors`；失败时包含 `error_type` 和至多 200 字符的
`error_summary`。成功时间在失败时保留，成功后清零连续失败次数。
错误摘要仅允许异常类型及受控的首行错误码，任意异常原文不写入状态或告警，避免
SQL、聊天正文、密钥或远端 payload 混入诊断。状态、计划和待发送包均先写私有临时文件
再原子替换；SQLite 游标使用事务，NAS 确认提交后才推进。

文字 worker 每轮执行 `/usr/bin/pgrep -x WeChat`，返回 0 时 `wechat_running=true`；
微信未运行时仍正常同步。每包（包括空包）携带 `mac_status`，仅含三个 worker 的
`stage/last_success_at/consecutive_errors/error_type/error_summary` 和微信运行状态。
`supervisor-config.json` 继续使用 `repository`、`python`、`supervisor` 三个字段；
`macos/supervisor.py` 的位置不变，无需修改、重编译或重新签名 `Launcher.swift`。

## NAS 心跳安装（由人工执行）

仓库仅提供 unit/timer 文件。先把更新后的 `tools/archive/` 下 Python 文件复制到
`vault/tools/`，保留 `macos/`、`nas/` 子目录，包含共享的 `archive_state.py` 和
`exclusion.py`；已有 ingest/decode/link/index 的命令行参数保持原样。
下文 `vault` 为 `/pool-safe/wechat-vault`。

1. 人工创建 `vault/heartbeat-config.json`，权限 `0600`，填入真实 target；不要将真实值
   写入仓库。`stale_minutes`、`error_threshold` 可省略，默认分别为 60 和 5。

   ```json
   {"target": "weixin:<id>", "stale_minutes": 60, "error_threshold": 5}
   ```

2. 确认 root 运行环境的 PATH 中能找到 `hermes`，并已由人工配置发送权限。
3. 复制并启用定时器（这些命令不会由本工单自动执行）：

   ```sh
   cp /pool-safe/wechat-vault/tools/nas/wechat-archive-heartbeat.service /etc/systemd/system/
   cp /pool-safe/wechat-vault/tools/nas/wechat-archive-heartbeat.timer /etc/systemd/system/
   systemctl daemon-reload
   systemctl enable --now wechat-archive-heartbeat.timer
   systemctl list-timers wechat-archive-heartbeat.timer
   ```

`ingest_delta.py` 使用 `<archive>.ingest.lock`，每次调用原子写入
`vault/mac-heartbeat.json` 的 `received_at` 与 `mac_status`，兼容没有状态字段的旧包。
定时器每 5 分钟检查一次：超时告警“Mac 同步已中断 N 分钟”，微信未运行告警一次，
任一 worker 连续失败达到阈值时告警并注明 worker 名和安全错误摘要。
同一条件持续时只告警一次，恢复时通知“已恢复”及中断分钟数；旧包缺少的状态不推断为恢复。
去重状态为私有、原子写入的 `heartbeat-state.json`。`hermes send -t TARGET -q 文本`
使用参数列表执行，任何一次发送失败都不更新本轮去重状态，下轮重试。

## NAS 历史清除（由人工执行）

**顺序必须是：先在 Mac 部署排除名单并确认新 worker 已生效，再复制名单到 NAS dry-run，
最后 apply。** 更新后应重启原有 app 的 Python 进程以载入代码，保持 app 签名与完全磁盘访问
授权不变；确认一轮文字、近期图片及全量媒体状态成功并核对私有待传计划。
不按此顺序，全量媒体的 NAS 对比会将已删除文件重新上传。

在 NAS 上用 root 执行，名单文件权限仍为 `0600`：

```sh
python3 /pool-safe/wechat-vault/tools/purge_conversations.py \
  --vault /pool-safe/wechat-vault --exclude /private/exclude.json
# 核对各路径的行数、文件数、字节数，以及 plan_digest 后再执行：
python3 /pool-safe/wechat-vault/tools/purge_conversations.py \
  --vault /pool-safe/wechat-vault --exclude /private/exclude.json \
  --apply --confirm PLAN_DIGEST_FROM_DRY_RUN
```

默认 dry-run 不修改 vault，不创建锁、报告或 WAL/SHM。无 WAL 时使用 SQLite 只读
immutable 连接；存在 WAL 时在系统临时目录复制主文件和 WAL 后读取，避免原库产生 SHM。
`row_bytes` 是待删除行各列的逻辑字节数，文件大小可精确列出，数据库 VACUUM 后的实际
空间变化在 apply 报告中给出。digest 覆盖名单 username、各处计数、大小及文件版本，
apply 持锁后重新计算；同步产生新数据也可能导致 digest 不匹配，需要重新 dry-run。

apply 会拒绝正在运行的 `decode_media.py`、`convert_wxgf.py`、`index_media.py`、
`link_media.py`（按 vault/tools 绝对路径 pgrep）。全程持有
`structured/chat-archive.sqlite3.ingest.lock` 和 `media-links.sqlite3.lock`，还独占
`media-transfer.lock`，阻止新接收/解码进程进入；四路媒体接收与批量解码平时共享后一把锁，
不会互相串行。近期解码、媒体索引与文字 ingest 使用 ingest 锁。全部相关脚本应一起更新。
占锁期间 Mac 的对应 SSH 调用失败，文字游标、近期媒体确认记录均不推进，之后自动重试。

所有数据库修改先在 vault 同文件系统的私有副本中准备，全部通过外键、`quick_check`、
归档 FTS5 `integrity-check` 和 VACUUM 后，逐个原子替换并清除旧 WAL/SHM。
归档会重建外部内容的 `message_search`，更新 `build_metadata` 计数并追加仅含时间和计数的
`purge:*` 记录。明文快照删除相应消息表及会话附属记录，群成员的联系人行保留。
归档消息同时按名单 username、`unresolved:Msg_<md5>` 和 `message_sources` 中的排除表
哈希选择，清理相应的未解析会话、正文、FTS 及关联记录，避免身份解析失败留下副本。
迁移快照 `migrate/unspportmsg.db` 的 `UnsupportMessage` 按 `from_user`、`to_user`
先判断群会话，再按私聊参与者匹配名单；保留群中由排除联系人发送的消息。
该库同样在副本中清理并 VACUUM，更新快照哈希；库结构或会话归属无法可靠识别时拒绝执行。
迁移消息中的媒体引用也参与共享文件归属判断。
`hardlink.db` 仅删除 `dir2id` 明确映射到排除会话或其 attach 目录的记录，未确认的行
保留并列入报告。

快照中的 `message/message_fts.db` 使用 NAS 无法加载的 `MMFtsTokenizer`，因此整文件删除；
其内容在构建归档时已保存在 `index_evidence`，清除时仅移除其中排除会话的证据。
快照 `manifest.json` 支持对象和列表两种格式。列表保持原格式，按相对 `databases/` 的
`path` 更新各项 `bytes`、`sha256`，移除已删除 FTS 的文件项；缺少必需字段、字段类型
错误或路径无效时，以 `invalid_snapshot_manifest` 拒绝执行。列表格式的完整
`database_hashes` 和仅含时间、计数的 `purged: {at, counts}` 写入同目录 `purge.json`
（`0600`，原子写入）；对象格式仍在 manifest 内更新已有哈希并写入这两个字段。
`verified.json` 保持对象格式，更新 `files` 为清除后 manifest 项数、`bytes` 为字节总和，
写入 `database_hashes`、`purged`，并按实际数据库哈希与新 manifest 的比较结果重新计算
`sha256_all_match`，保留其余字段。所有数据库与这些元数据均先暂存并完成校验，再发布
任何修改；元数据文件逐个原子替换。

媒体清除复用上述归属规则，同时清理 `media_files`、`decoded-media.sqlite3` 和链接库记录，
删除对应 decoded 输出及空目录。已核对三个解码脚本：decoded 的 `path` 相对 `media/msg`，
`output` 相对 `vault/decoded`。共享 token、共享文件、无法确定归属的文件保留；疑似路径
越界或符号链接会拒绝执行。

最终报告为 `vault/purge-<UTC时间>.json`（`0600`），包括每处前后计数和实际释放字节数；
空间变化为负表示新元数据导致该文件变大。输出不包含正文、备注或资源 payload。
清除需要容纳全部修改副本、VACUUM 临时空间和（有 WAL 时）最大库的系统临时空间。
原子性以单个文件为单位，多文件发布不是一个总事务；人工完整验证应在 NAS 的隔离副本
上先完成。VACUUM 消除 SQLite 活文件内的残页，不会清除 NAS 文件系统保留的旧快照或备份。

沙箱验证结果和需人工完成的环境验证见 [VALIDATION.md](VALIDATION.md)。
