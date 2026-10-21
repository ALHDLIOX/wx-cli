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
