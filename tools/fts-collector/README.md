# Linux local message collector (experimental)

Captures primary `message_N.db` tables and `message_fts.db` content tables in
parallel. Only records available on the Linux client are covered; this is **not a
complete phone-history backup**. FTS snippets may be incomplete, and media files
are not downloaded or decoded by this tool.

On Linux WeChat 4.1.13.26 one synthetic test appeared only in FTS while another
appeared only in the primary table. Independent SQLCipher queries confirmed this
split. Both sources are therefore captured and explicitly labeled; neither is
silently substituted for the other. This does not change `wx history` behavior.

## Install in the Linux guest

Requires Python 3, a configured `~/.wx-cli/config.json` and valid FTS database key.
Build wx-cli natively on Linux first, then:

```sh
mkdir -p ~/.local/lib/wx-fts-collector ~/.config/systemd/user
cp tools/fts-collector/collector.py ~/.local/lib/wx-fts-collector/
sh tools/fts-collector/build-library.sh . ~/.local/lib/wx-fts-collector/libsqlcipher.so
cp tools/fts-collector/wx-fts-collector.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now wx-fts-collector
```

The library is linked from this repository's vendored SQLCipher/OpenSSL build;
no third-party Python package or downloaded key-extraction binary is used.
Keys stay in the existing private configuration and are never command arguments.
Source database connections are read-only, with `query_only` also enabled.

## Behavior and storage

- Poll new source row IDs every 2 seconds, in batches of 500 without a global cap.
- Fully rescan every 60 seconds (and on collector startup) for late rows, edits,
  source resets, and name-map updates. Full scans scale with index size and may
  exceed the nominal poll interval on large histories. New message tables are discovered each cycle.
- Source snapshot per cycle; archive writes and cursor changes share a transaction.
- Identity includes source account/path/salt, table, row ID, local ID, sort sequence,
  session (or primary table) and message timestamp. Same-second records remain distinct. Repeated
  scans update changed payloads rather than appending duplicates.
- This is source-row deduplication, not semantic deduplication across primary/index
  sources. A message present in both has two evidence records, with `source` set to
  `message_primary` or `message_fts`; do not present archive row counts as chat counts.
- Primary payloads preserve raw `message_content` bytes as `content_hex`.
  Uncompressed text is also decoded into `content`. Compressed content (WCDB type 4)
  remains losslessly archived with `content=null` and `content_decoded=false`;
  subsequent zstd decoding is required for text queries over those records.
- Each configured source is checked independently; failures in one do not stop the
  other. Status reports `partial_error` and per-source results, including newly
  discovered primary shards lacking keys.
- A deleted source record remains archived. There is no automatic retention expiry.
- `~/.wx-cli/collector/archive.sqlite3` contains private message content in plaintext;
  directory is 0700, files are created with umask 0077. Existing files should be
  private before adoption. Do not publish, commit, or expose this directory.
- `status.json` reports counts and errors without message text or keys. `ok` means
  the configured local sources were read successfully; it does not prove WeChat is still logged
  in or receiving network messages. Check source freshness separately.
- Source errors retry, service crashes restart after 5 seconds, and a process lock
  prevents competing collectors on the same output directory. Account/key changes
  are loaded each cycle; new key acquisition is not automated.

```sh
cat ~/.wx-cli/collector/status.json
systemctl --user status wx-fts-collector
systemctl --user stop wx-fts-collector
python3 -m unittest discover -s tools/fts-collector -v
```

The service does not configure the PVE VM's boot policy, log into WeChat, migrate
phone history, notify an AI session, or send any messages. A separate acceptance
test is required for new private/group messages and for host reboot recovery.
