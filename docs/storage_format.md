# MiniDB storage formats and durability

A MiniDB directory holds a `MANIFEST`, a `LOCK` file, the active WAL, and zero or more SSTables. The contents of `LOCK` are unused. MiniDB holds an exclusive advisory lock on the open file throughout recovery and use. Keep the lock file in place and open the database through MiniDB. The operating system releases the lock when the handle closes or the process exits. Integers in all formats are unsigned and little-endian. Checksums use IEEE CRC-32.

## WAL version 1

The file starts with the six ASCII bytes `MINIDB` and a two-byte version number with value 1. Records follow in mutation order. Generation zero uses `wal.log`, as in the WAL-only engine. Later generations use `wal-{generation}.log`, with the ID written as 20 zero-padded decimal digits.

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

## SSTable version 1

An SSTable is named `sst-{id}.sst`, with the ID written as 20 zero-padded decimal digits. It starts with seven ASCII bytes `MINISST`, a one-byte version number with value 1, and an eight-byte table ID. Data blocks begin immediately after this 16-byte file header.

- **Data blocks.** Each block contains one or more records, followed by a four-byte CRC-32 of the record bytes. A record has the same one-byte operation, four-byte key length, four-byte value length, and raw key and value bytes as a WAL record. Checksums are computed per block. Keys are strictly lexicographically ordered by byte and unique within a table. A block targets 4096 record bytes. A larger record occupies its own block.
- **Index.** After the final block is an eight-byte block count followed by one entry per block. An entry contains an eight-byte absolute block offset, a four-byte first-key length, and the first key. The next offset, or the index start for the last block, defines each block's end.
- **Footer.** The final 12 bytes contain an eight-byte absolute index offset and a four-byte CRC-32 of all index bytes followed by the index offset bytes.

Opening a table validates its header, ID, index checksum, block bounds, and index ordering. The database retains the table path and index. Lookups binary-search the sparse index, open the selected file for the candidate block read, and verify the block checksum, record structure, and ordering. An unread data block can contain corruption discovered only when the block is read.

## Manifest version 1

The `MANIFEST` contains seven ASCII bytes `MINIMAN`, a one-byte version number with value 1, an eight-byte active WAL generation, an eight-byte live table count, the same number of eight-byte table IDs in ascending order, and a four-byte CRC-32 of the preceding bytes. The highest live table ID has newest precedence. ID 0 is reserved for the legacy WAL. A successful flush increments the active generation and uses that generation as the new table ID.

## Publication and recovery

Each staged file uses the destination name with a `.tmp` suffix. If the manifest is absent and no numbered storage files exist, MiniDB opens or creates `wal.log`, synchronizes it and the directory ancestry, and publishes a generation-zero manifest. This also migrates WAL-only databases. Upgrade is one-way for writers. A WAL-only version ignores the manifest and can write to `wal.log` even after a newer version has rotated to a numbered WAL. A manifest missing alongside numbered artifacts, an unreadable manifest, or a missing file named in the manifest produces an error.

A flush holds the active MemTable intact while it builds a new table and a new empty WAL. MiniDB synchronizes each staged file before renaming it, then synchronizes the directory to persist both names. It synchronizes the staged manifest, atomically replaces `MANIFEST`, and synchronizes the directory again. Only after the replacement is durable does it switch the in-memory state and attempt to remove the old WAL. An error during manifest rename or subsequent directory synchronization leaves publication uncertain. MiniDB keeps both generations and requires reopening before another mutation or flush. A failure before the rename attempt leaves the current generation active and permits retrying the flush. Recovery uses whichever complete manifest the filesystem exposes and synchronizes the selected state before attempting to remove obsolete files.

On every successful open, MiniDB synchronizes the active WAL and each resolved directory from the database directory up to the root. If any ancestor cannot be opened or its filesystem rejects directory synchronization, `open` returns the error. An orphan table, WAL, or staging file is disposable only after the manifest and its referenced files are loaded and the chosen directory state is synchronized. Obsolete-file cleanup is best effort and is retried on open.

Recovery replays only the active WAL, in file order, using the format's limits. A DELETE stores a tombstone. A later PUT can replace it. Recovery truncates and synchronizes an incomplete final record after the last valid record. An incomplete record header is considered recoverable only when the available bytes are consistent with the format. Invalid headers, independently detected length corruption, and checksum mismatches on complete records cause an explicit error. Corruption in a complete final record leaves the WAL untouched.

## Guarantees and assumptions

- A successful mutation means MiniDB appended its record, synchronized the WAL through `File::sync_all`, and applied it to the MemTable in that order.
- These guarantees assume a Unix filesystem and storage stack that honor file synchronization, directory synchronization, and atomic rename. A synchronization or locking error is returned to the caller. Support has been exercised on macOS.
- A failed or interrupted mutation can be absent or can appear after reopen if its complete valid record reached the WAL. An append or sync error leaves the current MemTable untouched and stops further mutations on that handle until reopen.
- An automatic flush happens after the mutation has been synchronized and applied. `Error::AutoFlushFailed` means that specific mutation is durable and visible. If the MemTable is still at or above the threshold on a subsequent valid mutation, MiniDB flushes before appending the new record. An error from this retry leaves the new mutation unapplied.
- The directory is owned by one cooperative MiniDB handle at a time. Other programs must avoid editing, replacing, or deleting database files while MiniDB is using them.
- The active WAL is append-only apart from incomplete-tail repair and is replaced on each nonempty flush. The MemTable threshold counts key bytes, current value bytes, and inline key and entry metadata. It is an estimate and excludes allocator and tree-node overhead. Without compaction, the number of retained table indexes grows with the number of flushes.
