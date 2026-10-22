# 清除工具 NVMe 工作目录验证报告

本次相对 base `5128eb1` 的改动仅位于 `tools/archive/`。
验证环境：Python 3.13.5、SQLite 3.46.1，使用本地临时目录和虚构数据。

执行命令：

```sh
python3 -m unittest discover -s tools/archive -v
```

最终结果（退出码 0）：

```text
Ran 70 tests in 4.489s

OK
```

70 项全部通过，0 项跳过。完整输出保存在工作树外的
`../out/archive-unittest.log`。

本次实现及回归覆盖：

- 新增 `--work-dir`，dry-run 和 apply 均支持；校验绝对路径、存在的目录、
  非符号链接、当前用户属主及不宽于 `0700` 的权限。
- 在指定目录创建 `0700` 私有子目录，在任何 SQLite 连接之前设置进程内
  `SQLITE_TMPDIR`、`TMPDIR`、`tempfile.tempdir`；dry-run 的 WAL 读取副本也位于其中。
  不指定工作目录时保留原有处理位置，apply 的 `SQLITE_TMPDIR` 指向 vault 私有临时目录。
  测试检查每次连接时的目录设置、权限，以及退出后的环境恢复和目录清理。
- 对相同夹具分别执行默认 apply 和外部工作目录 apply，固定时间后逐文件比较数据库、
  manifest、verified.json、purge.json 和媒体结果的 SHA256，结果一致。
  同时验证报告中的数据库统计、文件统计及释放字节一致。
- apply 准备数据库副本前，检查工作文件系统可用空间大于 S × 2.5 + 2 GiB，
  vault 文件系统可用空间大于 S + 2 GiB；S 为待修改数据库主文件大小之和。
  模拟两处空间为零或恰等于阈值，均以对应空间错误码拒绝；未开始准备或回拷，
  原有 vault 文件的大小、修改时间及哈希均不变。
- 所有数据库完成删除、FTS rebuild、VACUUM、外键及 SQLite 校验，元数据完成暂存后，
  将数据库和元数据复制到各自目标同目录的 `0600` 临时文件。
  测试检查文件 fsync、目录 fsync 均早于回拷哈希校验，发布时源文件和目标位于同目录。
- 分别注入早期数据库回拷、较晚元数据回拷的哈希不一致，均在发布前中止；
  原有 vault 文件的大小、修改时间及哈希不变，所有回拷文件及工作子目录均已清理。
  持久锁文件按既有协议预先创建并保留，不删除并发协调锁。
- 在第二个回拷文件写入后注入 OSError、KeyboardInterrupt 和 SystemExit，
  验证已完成及未完成回拷文件全部清理，未发布数据库、元数据、报告或删除媒体。
  CLI 将 SIGTERM 转为退出异常，使其执行同样的 finally 清理路径。
- stderr 按行输出可解析 JSON，覆盖复制、删行、FTS rebuild、VACUUM、校验、
  回拷、发布、媒体删除和报告阶段；全部校验先于回拷、全部回拷先于发布。
  日志不包含夹具正文、名单 username 或媒体文件名；stdout 保持结果 JSON。
- 使用真实 CLI 验证 `--work-dir` dry-run，含尚未 checkpoint 的 WAL 内容；
  vault 不变、工作目录无残留、stdout 无正文、stderr 无 apply 阶段日志。
- 原有归档、增量同步、行号复用、排除名单、媒体、解码、心跳和清除测试全部通过。
  README 已补充 NVMe 推荐命令、路径限制、空间公式、错误码和进度输出说明。

另执行 `git diff --check`、全部归档 Python 文件 AST 语法解析及改动路径检查，均通过。
未修改 Rust、Cargo、UI、门禁配置或新增依赖；未提交、推送、部署、读取秘密或操作生产资源。
测试中的 root 身份检查和进程探测使用模拟；所有数据库修改仅操作临时夹具。
未在真实 NAS/NVMe/ZFS 上进行容量或性能实测。
