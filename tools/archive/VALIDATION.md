# 微信归档同步工单验证报告

本次相对 base `c7fb43f` 完成：Mac supervisor 直接 import 调用、每轮关闭文件锁、
原子状态及安全错误摘要；会话排除及共享媒体归属判断；NAS ingest 心跳、告警去重与恢复；
dry-run / digest 确认的历史清除工具、配套测试和人工操作文档。

验证时间：2026-10-21（沙箱）。Python 3.13.5，SQLite 3.46.1。

执行仓库要求的命令：

```sh
python3 -m unittest discover -s tools/archive -v
```

最终结果（退出码 0）：

```text
Ran 41 tests in 1.368s

OK
```

41 项全部通过，0 项跳过。当前环境已有 Pillow、cryptography；已有媒体测试在缺少这两项
可选依赖时明确跳过，其余模块不放宽检查。完整日志位于工作树外的
`../out/archive-unittest.log`（路径以工作树根目录为基准）。没有新增第三方依赖。

覆盖内容：

- 既有归档构建、Zstandard 解码、幂等 ingest、同秒消息与编辑、FTS、媒体解码和关联测试。
- 同一进程连续运行 worker、失败后再运行时释放锁；成功时间保留、连续错误计数清零与递增、
  摘要不含 payload；直接调用媒体模块且不修改全局 subprocess。
- 排除名单缺失、坏格式和权限错误；排除表在身份解析前跳过且不写游标，过滤资源引用，
  每轮重读；微信未运行仍传输；SSH 失败不推进游标、下一轮重传。
- 近期图片目录过滤、全量目录剪枝、专属与共享 token、原始文件名加字节数归属、未确认计数。
- 心跳空包及旧包、文件权限、ingest 锁；假时间和假发送器覆盖超时、微信未运行、三个 worker
  阈值、去重、恢复时长、发送失败重试；实际发送命令采用参数列表。
- 小型虚构夹具中的归档所有指定表、外部内容 FTS5、计数元数据、快照消息/语音/资源/session/
  contact/hardlink 库、删除私有分词器 FTS 整文件、快照新哈希及 purged 记录。
- 使用真实 ingest 保存已有源行的文件及视频编辑，验证旧快照和旧正文之外的
  `incremental_edits` 引用能保留共享媒体及其索引；坏 JSON、缺字段、非法 hex、无效压缩、
  无法解码证据令专属候选文件记为未确认；坏 appmsg 仅在原文命中文件名（含 XML
  转义）时保留该文件，无关失败不影响排除。排除会话的失败证据不影响判断，输出不泄漏正文。
- 使用真实 ingest 将新来源行合并到已有消息，确认正文只存入
  `message_sources.variant_text`、不生成编辑记录；dry-run 和 apply 均保留变体中引用的
  共享文件、视频、缩略图及 `media_files` 索引。变体为非 XML、截断 XML 或非法大小时，
  按原文中的文件名逐个决定是否保留文件及索引，报告不泄漏正文。
- 通过真实 `Builder.primary()` 生成稀疏 Name2Id 下的 `unresolved:Msg_<md5>`，验证 apply
  清除正文、来源、FTS、会话、资源、增量引用和链接记录，外键与 FTS 完整性检查通过。
- 迁移快照 `migrate/unspportmsg.db` 覆盖私聊两个方向、群聊两个方向、保留群中排除联系人
  的消息、共享文件保留、VACUUM 和 manifest/verified 哈希更新；用真实
  `Builder.unsupported()` 构建相应归档副本，验证同步清除正文及 FTS。
  不支持的迁移库结构和缺失身份在 dry-run/apply 时拒绝，原数据与报告保持不变。
- 媒体、decoded 输出及清单、链接库、空目录清理；共享视频和共享/未知原名文件保留；
  保留群成员的 contact 行；未确认 hardlink 记录保留。
- dry-run 函数与 CLI 不改 vault 的文件、内容及 mtime；digest 变化、活动媒体进程、三把锁、
  越界 decoded 路径均拒绝执行；WAL 副本读取不改源文件；超过单批 SQL 参数数目的清除；
  校验失败时不发布副本；VACUUM 后 freelist 为 0，外键、quick_check 和 FTS integrity-check 通过。
- 在本地子进程执行真实 stream_receiver 协议，校验内容和完成状态；共享锁允许传输，
  独占锁拒绝传输且不改状态文件。测试不调用真实 SSH、hermes 或生产资源。

另执行 `git diff --check`、全部归档 Python 的 AST 语法解析以及改动路径检查，均通过。
改动均位于 `tools/archive/` 的 Python、文档和明确要求提供的 systemd unit/timer。
未修改 Rust、Cargo、Launcher.swift、门禁配置，没有提交、推送、安装 unit 或部署。

沙箱外的完整验证由人工完成：Mac 上原有 app / 完全磁盘访问授权下的真实 SQLCipher
读取和三轮 worker、真实名单生效及共享媒体保留；NAS 隔离 vault 副本上的真实快照格式、
磁盘空间与权限、进程识别、跨进程锁竞争、清除后哈希和查询复核；真实 hermes 发送与
systemd timer 运行。按 README 先在 Mac 部署排除名单并确认生效，再到 NAS dry-run 和 apply。
清除以单文件原子替换发布，多文件不是一个总事务；文件系统旧快照及备份需人工管理。
