# 增量同步行号回退与复用验证报告

本次相对 base `aa2d2a4` 的改动仅位于 `tools/archive/`。
验证环境：Python 3.13.5、SQLite 3.46.1，使用本地临时目录和虚构数据。

执行命令：

```sh
python3 -m unittest discover -s tools/archive -v
```

最终结果（退出码 0）：

```text
Ran 62 tests in 3.981s

OK
```

62 项全部通过，0 项跳过。完整输出保存在工作树外的
`../out/archive-unittest.log`。

本次回归覆盖：

- 末尾删除后按表降低游标，其他表继续；随后复用尾部行号时按新的完整行指纹重发。
- `ROWID_ROLLBACK_LIMIT = 1000` 边界允许回退；超过阈值或清空消息表时仅阻塞该表，
  记录安全日志及 `blocked_tables`，该表不发消息、不改游标，健康表继续同步。
- salt 改变仍以 `source_generation_changed` 中断整轮；SSH 失败不提交游标回退，
  错误状态保留已完成的行号检查计数，成功重试后再提交。
- 在零及非零 server ID 下分别改变 local_id、timestamp、server_id，即使正文相同也
  创建新消息；多次复用保留全部旧消息及历史来源，重传幂等。
- 三项身份一致、正文改变时仍写入 `incremental_edits`；来源迁移和新消息写入同一事务，
  写入中途异常会同时回滚。校验消息、来源与会话计数、外键、FTS 索引完整性。
- supervisor 在文字 worker 失败后通过相同 SSH 命令发送最新状态空包，包含 self_user、
  连续错误次数、微信状态及行号诊断字段；补发失败仅记安全日志，后续轮次正常执行。
- 状态空包仅更新心跳，归档文件内容不变，不创建缺失的归档数据库。
- heartbeat 对阻塞表告警一次、持续异常去重、缺失字段不误判恢复，归零后发恢复通知。
- purge 同时按历史来源的排除表哈希及关联消息所属会话清除 superseded_sources，
  保留其他历史消息与来源；更新来源及解码统计，外键与 FTS 检查通过。
- 历史来源中的 variant_text 继续参与共享媒体归属判断，避免误删保留会话的共享文件。

原有归档、解码、排除名单、媒体、心跳、清除测试均通过。
`build_archive.py` 和 `query_archive.py` 未修改：原有构建去重测试保持通过，新增回归使用
真实查询 CLI 验证复用后历史查询、搜索及统计均能访问保留的三条消息。

另执行 `git diff --check`、全部归档 Python 文件 AST 语法解析及改动路径检查，均通过。
没有修改 Rust、Cargo、UI、门禁配置或新增依赖；未提交、推送、部署、读取秘密或操作生产资源。
所有 SSH、微信进程探测及通知发送均在相应测试中模拟，不触达真实 Mac/NAS；
真实 SQLCipher 及跨机器传输未在沙箱执行。
