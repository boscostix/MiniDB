# Write-ahead log format and durability

A MiniDB directory holds `wal.log` and a `LOCK` file. The contents of `LOCK` are unused. MiniDB holds an exclusive advisory lock on the open file throughout recovery and use. Keep the lock file in place and open the database through MiniDB. The operating system releases the lock when the handle closes or the process exits.

## WAL version 1

All integer fields are unsigned and little-endian. The file starts with the six ASCII bytes `MINIDB` and a two-byte version number with value 1. Records follow in mutation order.

| Field | Size | Meaning |
| --- | --- | --- |
| Operation | 1 byte | 1 for PUT, 2 for DELETE |
| Key length | 4 bytes | Number of key bytes, at least 1 |
| Value length | 4 bytes | Number of value bytes, 0 for DELETE |
| Header checksum | 4 bytes | IEEE CRC-32 of the previous nine bytes |
| Key | Key length | Raw key bytes |
| Value | Value length | Raw value bytes, possibly empty for PUT |
| Payload checksum | 4 bytes | IEEE CRC-32 of the concatenated key and value bytes |

The header checksum covers the operation and both lengths, so corrupted sizing fields are checked before recovery uses them to find the end of a record. Recovery checks that the declared record fits within the file before allocating its payload. The reader buffers I/O and decodes one record at a time while rebuilding the MemTable.

## Publication and recovery

When there is no published WAL, MiniDB writes the header to `wal.log.tmp`, synchronizes that file, and renames it to `wal.log` within the database directory. An unpublished staging file can be recreated on the next open. On every successful open, MiniDB synchronizes the WAL and each resolved directory from the database directory up to the root. This covers directory names created by the current open or a previous interrupted open. If any ancestor cannot be opened or its filesystem rejects directory synchronization, MiniDB returns the error from `open`.

Recovery replays records in file order. A DELETE stores a tombstone. A later PUT can replace it. Recovery truncates and synchronizes an incomplete final record after the last valid record. An incomplete record header is considered recoverable only when the available bytes are consistent with the format. Invalid headers, independently detected length corruption, and checksum mismatches on complete records cause an explicit error. In particular, corruption in a complete final record causes an error and leaves the WAL untouched.

## Guarantees and assumptions

- A successful mutation means MiniDB appended its record, synchronized the WAL through `File::sync_all`, and applied it to the MemTable in that order.
- These guarantees assume a Unix filesystem and storage stack that honor file synchronization, directory synchronization, and atomic rename. A synchronization or locking error is returned to the caller. Support has been exercised on macOS.
- A failed or interrupted mutation can be absent or can appear after reopen if its complete valid record reached the WAL. An append or sync error leaves the current MemTable untouched and stops further mutations on that handle until reopen.
- The directory is owned by one cooperative MiniDB handle at a time. Other programs must avoid editing, replacing, or deleting database files while MiniDB is using them.
- The WAL is append-only apart from incomplete-tail repair. It has no rotation or compaction, so startup work grows with WAL size, and the recovered state must fit in memory.
