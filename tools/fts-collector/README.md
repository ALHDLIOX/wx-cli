# Linux local FTS collector (experimental)

Captures records already present in the logged-in user's local `message_fts.db`.
This is an index archive, **not a complete chat history backup**. Text snippets may
be incomplete; media and unindexed messages are outside its coverage.

On Linux WeChat 4.1.13.26 the second synthetic test appeared in FTS and session
metadata but not the primary message table. A separate read-only SQLCipher query
and an isolated DB+WAL copy confirmed the absence. This collector does not claim
to fix `wx history` or silently substitute index snippets for full messages.

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
  exceed the nominal poll interval on large histories.
- Source snapshot per cycle; archive writes and cursor changes share a transaction.
- Identity includes source account/path/salt, table, row ID, local ID, sort sequence,
  session and message timestamp. Same-second records remain distinct. Repeated
  scans update changed payloads rather than appending duplicates.
- This is source-row deduplication, not semantic deduplication across index tables.
- A deleted source record remains archived. There is no automatic retention expiry.
- `~/.wx-cli/collector/archive.sqlite3` contains private message content in plaintext;
  directory is 0700, files are created with umask 0077. Existing files should be
  private before adoption. Do not publish, commit, or expose this directory.
- `status.json` reports counts and errors without message text or keys. `ok` means
  the local index was read successfully; it does not prove WeChat is still logged
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
