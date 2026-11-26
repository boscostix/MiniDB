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

## Manifest versions 1 and 2

A version 2 manifest has the following fields in order.

| Field | Size | Meaning |
| --- | --- | --- |
| Magic and version | 8 bytes | Seven ASCII bytes `MINIMAN` followed by a byte with value 2 |
| Active WAL generation | 8 bytes | ID of the WAL to replay, with 0 for the legacy WAL |
| File ID high-water mark | 8 bytes | Upper bound on the active WAL generation and every live table ID |
| Live table count | 8 bytes | Number of live table IDs that follow |
| Live table IDs | 8 bytes each | Unique, nonzero IDs in oldest-to-newest lookup precedence order |
| Checksum | 4 bytes | IEEE CRC-32 of all preceding manifest bytes |

MiniDB can also read Milestone 3's version 1 manifest. Its fields have the same layout without the high-water mark, and its table IDs must be in strictly ascending order. MiniDB uses the active WAL generation as the initial high-water mark for a version 1 manifest. New publications use version 2. Older versions of MiniDB cannot open the database once it uses version 2.

A flush allocates a new ID above the handle's current high-water mark and uses it for both the new WAL generation and the new table. A compaction reserves a new table ID without changing the active WAL generation and skips occupied SSTable file names and staging names. A concurrent flush can publish a greater ID before the worker publishes its output. The high-water mark prevents reuse of previously published IDs, and the explicit manifest order determines lookup precedence even when IDs are out of order.

## Publication and recovery

Each staged file uses the destination name with a `.tmp` suffix. If the manifest is absent and no numbered storage files exist, MiniDB opens or creates `wal.log`, synchronizes it and the directory ancestry, and publishes a generation-zero manifest. This also migrates WAL-only databases. Upgrade is one-way for writers. A WAL-only version ignores the manifest and can write to `wal.log` even after a newer version has rotated to a numbered WAL. A manifest missing alongside numbered artifacts, an unreadable manifest, or a missing file named in the manifest produces an error.

A flush request holds the writer gate while the flush worker builds a new table from the published MemTable and creates a new empty WAL. Reads continue against the published view during staging and synchronization. MiniDB synchronizes each staged file before renaming it, then synchronizes the directory to persist both names. It synchronizes the staged manifest, atomically replaces `MANIFEST`, and synchronizes the directory again. Once the replacement is durable, the worker takes the view write lock, publishes the new table and empty MemTable together, then attempts to remove the old WAL. An error during manifest rename or subsequent directory synchronization leaves publication uncertain. MiniDB keeps both generations and requires reopening before another mutation or flush. A failure before the rename attempt leaves the current generation active and permits retrying the flush. Recovery uses whichever complete manifest the filesystem exposes and synchronizes the selected state before attempting to remove obsolete files.

### Background compaction

The worker starts when at least four tables are live after open or a flush. It snapshots the four oldest and streams their checked data blocks in byte-key order, keeping one current block per input and the output's sparse index. For each key it copies the newest value, including an empty value. It omits a winning tombstone because the merge includes every older table. Any table outside the merge keeps its values and tombstones. A merged set with no remaining values produces no output file.

The worker synchronizes the staged output, renames it into place, and synchronizes the directory. It then takes the metadata lock, updates the current manifest by replacing the merged prefix, and uses the same staged manifest synchronization, atomic replacement, and directory synchronization as a flush. Any table appended by a concurrent flush stays in the newer suffix. The active WAL is unchanged. After publication is durable, new lookups see the new table set. Lookups that selected the previous set can still read it, and MiniDB reclaims inputs after the last reference is released.

Flush and compaction publication share the persistence lock. A merge already in progress can stream and write its output while a flush stages its files. Both workers release the published view lock before any file I/O.

Errors before manifest replacement leave the inputs live. During replacement or the following directory sync an error makes publication uncertain. MiniDB keeps both possible file sets, continues reading the previously selected tables, and rejects further mutations and flushes until reopen. The worker stops after its first error and reports it through `compaction_error` and `close`. An unexpected worker panic is reported by `close`. Close and final drop stop scheduling and join both workers before unlocking the directory. A flush worker panic unblocks its caller and disables further writes until reopen. Explicit close reports worker panics and the recorded compaction error, and final drop discards worker errors.

### Reopen

On every successful open, MiniDB synchronizes the active WAL and each resolved directory from the database directory up to the root. If any ancestor cannot be opened or its filesystem rejects directory synchronization, `open` returns the error. An orphan table, WAL, or staging file is disposable only after the manifest and its referenced files are loaded and the chosen directory state is synchronized. Obsolete-file cleanup is best effort and is retried on open.

Recovery replays only the active WAL, in file order, using the format's limits. A DELETE stores a tombstone. A later PUT can replace it. Recovery truncates and synchronizes an incomplete final record after the last valid record. An incomplete record header is considered recoverable only when the available bytes are consistent with the format. Invalid headers, independently detected length corruption, and checksum mismatches on complete records cause an explicit error. Corruption in a complete final record leaves the WAL untouched.

## Operation ordering and visibility

A directory admits one open MiniDB handle, which can be shared across threads in the process. Point mutations and flush requests acquire a single writer gate. If a call returns before a later call begins, that later call observes the earlier call's effects or a newer update. When writers overlap, their gate acquisition determines their order. The gate provides no FIFO scheduling guarantee.

A mutation appends and synchronizes its WAL record before taking the view write lock to publish the MemTable update. A lookup takes a shared lock to observe a coherent MemTable and live table set. On a MemTable miss, it pins the live table set and searches it after releasing the lock, so later updates or file retirement cannot alter that lookup's snapshot. Flush and compaction publish coherent replacements that preserve the logical contents of the read view. An overlapping lookup can return any state it observed during the call, including the previous value while a mutation is syncing. A series of lookups may observe different views.

Manual flush and automatic flush callers wait for their request to finish. A manual flush covers mutations that precede it in writer-gate order. Flushes stage files while holding the persistence lock, so other writers and compaction publication can wait on disk I/O. The read view lock is held for memory access and publication only.

## Guarantees and assumptions

- A successful mutation means MiniDB appended its record, synchronized the WAL through `File::sync_all`, and applied it to the MemTable in that order.
- These guarantees assume a Unix filesystem and storage stack that honor file synchronization, directory synchronization, and atomic rename. A synchronization or locking error is returned to the caller. Support has been exercised on macOS. The crash tests terminate processes at persistence boundaries and use the documented filesystem guarantees for power-loss durability.
- A failed or interrupted mutation can be absent or can appear after reopen if its complete valid record reached the WAL. An append or sync error leaves the current MemTable untouched and stops further mutations on that handle until reopen.
- An automatic flush happens after the mutation has been synchronized and applied. `Error::AutoFlushFailed` means that specific mutation is durable and visible. If the MemTable is still at or above the threshold on a subsequent valid mutation, MiniDB flushes before appending the new record. An error from this retry leaves the new mutation unapplied.
- The directory is owned by one cooperative MiniDB handle at a time. Other programs must avoid editing, replacing, or deleting database files while MiniDB is using them.
- The active WAL is append-only apart from incomplete-tail repair and is replaced on each nonempty flush. The MemTable threshold counts key bytes, current value bytes, and inline key and entry metadata. It is an estimate and excludes allocator and tree-node overhead. The worker has a fixed fan-in of four and no rate limit. Under heavy flushing, the number of live tables can grow while the worker catches up. An output file can temporarily coexist with all inputs, and older data can be rewritten in successive merges.
