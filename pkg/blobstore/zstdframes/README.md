# Compressed primary CAS storage

Local block-device CAS can store validated Zstandard frames as its primary
records. Logical CAS hashes and sizes still describe the uncompressed bytes.

```text
compressed upload -> frontend forwards -> storage decodes, validates and encodes
                                      -> publish indexed compressed record
stored frames -> storage ByteStream -> frontend ByteStream -> client
                 full compressed reads retain the same bytes at each hop
```

See [`LocalBlobAccessConfiguration.CASCompression`](../../proto/configuration/blobstore/blobstore.proto)
for configuration, staging and codec budgets, persistent-state ownership, and
fresh-store/rollback procedures. Compression at rest is optional. The shared
ByteStream/client, reader-completion and persistent-marker changes also apply
when it is disabled.

## Representation and integrity

The v1 format is independent compressed frames followed by a bounded JSON index
with hex digests and a 48-byte checksummed version footer. The index binds the
record to the requested logical hash, digest function and size, and records each
frame's logical digest, encoded SHA-256, offset and lengths. Publication requires
complete logical validation; a partial staging file is never a committed record.
Checksums detect accidental corruption, not hostile replacement of data and index.

**Format decision:** retain this JSON v1 index. Its bounded parser and binding
checks are already part of the format contract. At the integrated 1 MiB frame
size, a 256 MiB blob has 256 entries. A binary index would reduce parsing and
allocation costs, but is not necessary for encoded transfers or indexed reads.
This accepts JSON overhead on record consumption; presence checks do not parse
indexes or read records. There is no index cache. An incompatible future format
requires retiring this cache, rather than silently interpreting existing records.

Frame sizes range from 4 KiB to 4 MiB, with at most 65,536 frames and 32 MiB of
serialized index. Encoding enforces a tighter frame-count-proportional budget.
The integrated 1 MiB frame size supports at most 64 GiB per blob; upload admission
can impose a lower limit. Tiny blobs of at most 4 KiB stay raw when indexing would
expand them. Larger incompressible records retain the index for bounded random
reads. Built-in encoders write known-size frames rather than requiring their
streaming history window to decode each small frame.

Allocation and refresh use physical record sizes. Refresh verifies and copies the
compressed representation without codecs. Ordinary replication to another backend
uses its normal Buffer contract. `FindMissing` and duplicate detection retain the
base store's batched metadata lookup and refresh locking, with no extra probes.

## Read and forwarding behavior

- Whole compressed reads validate encoded frame checksums and use no codecs at
  either server. The final client still validates the logical CAS digest.
- Compressed offsets are logical. Aligned reads forward stored frames; an
  unaligned first frame is decoded and re-encoded once. Decoder and encoder leases
  are released before sending bytes, and are never held together.
- Stored `ReadAt` and direct identity-offset reads use the index to skip preceding
  frames. Identity-only network hops retain logical decoding. Each touched frame
  is decoded and logically validated in full.
- Metrics, sharding and deadline decorators preserve encoded forwarding. Retrying
  decorators, including mirrored and read-caching paths, use logical fallback.
- Untrusted encoded uploads have a separate API. They cannot enter the trusted
  read fast path. The receiving storage server validates the logical digest before
  publishing anything; frontends bound incoming encoded bytes without decoding.
- Backend I/O errors do not establish corruption. Verified frame/index corruption
  invalidates the containing block under the existing local-store policy.
  Unsupported versions return `FAILED_PRECONDITION` without eviction and remain
  present in metadata. Stop and correct the format/configuration mismatch or
  retire that cache; presence checks and duplicate uploads do not repair them.

`Reader` supports independent concurrent frame reads over an immutable `ReaderAt`.
This is a local representation API, not a new parallel frame-download protocol.
It does not change REAPI chunk negotiation or require clients to understand the
on-disk index. Index loading is deferred until consumption, outside the store's
metadata lock. Clones retain their block references until consumption completes.

## Metrics

Metrics are process-wide, with fixed labels and no digest or instance labels.

| Metric suffix under `buildbarn_cas_compression_` | Meaning |
| --- | --- |
| `staging_reserved_bytes` | Reserved output bytes, including upload-codec waits |
| `staging_wait_duration_seconds{outcome}` | Admission latency: admitted, canceled, timeout |
| `active_requests` | Receiving and waiting Put requests, including duplicates |
| `rejected_requests_total{reason}` | request_queue, staging_timeout, staging_budget |
| `puts_total{outcome}` | Successful indexed, tiny_raw, detected-duplicate Put paths |
| `logical_bytes_total{outcome}` | Successful logical bytes by outcome |
| `physical_bytes_total{outcome}` | Representation bytes accepted by successful indexed and tiny_raw Puts |
| `read_integrity_errors_total{reason}` | corrupt_frame, invalid_index, unsupported_version; excludes device failures |

Use physical/logical byte rates for indexed and tiny_raw outcomes to measure
representation ratio; exclude duplicate logical bytes. These counters do not measure
actual device writes: concurrent uploads or hierarchical instance grants can deduplicate
after encoding inside the base store. Transfer selection is counted by
`buildbarn_bytestream_compression_transfers_total{direction,path}`: direction is
read or upload, and path is compressed_passthrough or logical_fallback. Reads are
counted at the server and forwarded uploads at the CAS client. These count attempts,
including failures; use existing RPC status/latency and codec-pool metrics alongside
them. Samples aggregate backends within a process.
