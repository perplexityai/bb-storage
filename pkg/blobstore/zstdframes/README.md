# Compressed primary CAS storage

Local block-device CAS storage can retain validated Zstandard representations as
its primary records. Full compressed ByteStream reads forward the stored frame
bytes through storage and frontend servers without acquiring encoders or decoders.
The logical CAS digest remains the hash and size of the uncompressed bytes.

```text
compressed upload -> frontend forwards unchanged -> storage validates + frames
                                                  -> publish local record
local compressed record -> storage ByteStream -> frontend ByteStream -> client
                           same compressed frames throughout the full read
```

## Configuration and storage ownership

Set `local.casCompression` on a block-device CAS backend:

```json
{
  "casCompression": {
    "stagingDirectory": "/var/lib/buildbarn/staging",
    "maximumStagedBytes": "1073741824",
    "maximumConcurrentWrites": "8"
  }
}
```

The directory must already exist on a disk filesystem that supports unlinking an
open file. Avoid memory-backed filesystems. This is a fragment of the local
backend configuration, not a complete server configuration. In-memory blocks and
non-CAS storage do not support this option. Absent configuration retains raw
storage. To forward compressed reads across a frontend's internal gRPC hop, also
set that backend's `grpc.enableCompression` and advertise `ZSTD` at the servers.

Already-present blobs drain encoded upload bytes through `FinishWrite` without
acquiring codecs. This preserves compatibility with older clients that treat an
early successful response as a failed send. Close and background-task errors
remain observable. Duplicate detection runs inside the authorized storage Put
path; failures refreshing an existing record do not prevent a repair upload.

Uploads stream through validation and frame encoding into one unlinked output
file. Encoding starts as frames arrive; there is no input spool or replay pass.
The conservative output reservation is roughly 1.125 times logical size plus
framing/index overhead. A 256 MiB upload reserves about 289 MiB, regardless of its
actual compression ratio. A 1 GiB budget can admit three such reservations. Size
for concurrent maximum-sized writes, including repair/replication; a nested repair
into the same destination needs an additional reservation and may time out if
all staging capacity is held by its callers. Blobs that can never fit return
`INVALID_ARGUMENT`; configuration too small for an empty record fails at startup.

`maximumConcurrentWrites` is the upload codec concurrency budget. Uploads have
separate encoder and decoder pools, independent of read-side codecs. The default
counts equal that budget, with fastest encoding, a 1 MiB encoder window, and an
8 MiB decoder window. Encoded uploads requiring larger windows (for example,
some `zstd --long` streams) are deliberately rejected with `INVALID_ARGUMENT`.
Increase the upload decoder window explicitly if such clients must be supported,
and account for the additional memory per concurrent decoder. Optional
`uploadZstdPool` overrides these settings using the Zstd pool configuration schema; codec counts cannot exceed the budget. Both pools
contribute to total process memory. Startup checks that read decoders can decode
the upload pool's stored frames before opening the device.

Slow compressed clients hold upload decoders while receiving. They cannot consume
read-side codec capacity. At most four times the budget may be receiving or waiting,
including duplicates; a full request queue returns `RESOURCE_EXHAUSTED`. Requests
wait up to 30 seconds for staging bytes, or their own deadline if earlier.
Admission permits smaller uploads to pass a larger waiter when they fit; a large
upload can time out under sustained smaller traffic. Upload codec acquisition has
its own 30-second cap, shortened by the caller deadline. That cap does not limit
an already-acquired codec or interrupt network reads. Identity streams do not
acquire an upload decoder; they acquire an encoder only after a logical frame is available. Encoder leases
release before receiving further input or sending output. There is no extra write
semaphore spanning source reads, which would deadlock synchronized replication
clones sharing the same destination.

Whole-input retries reset the output file and retain source error handlers,
backend integrity callbacks, and completion errors through validation. Nothing is
published until all checks succeed. A crash between temporary-file creation and
unlink can leave an empty named file; payload writes begin only after unlink.

Allocation and refresh use the physical record length; CAS keys and API sizes
remain logical. Refresh verifies the compressed representation without codecs.
Tiny records of at most 4 KiB stay raw when indexing would expand them. Larger
records retain the frame index, including incompressible data, to preserve bounded
random reads. Ordinary copies into another backend use logical bytes.

Indexed records use independent 1 MiB logical frames, followed by a bounded index and
versioned footer. This is the primary copy, not a second read cache. Uploads are
validated and encoded at storage; arbitrary client-supplied encoded indexes are
never accepted. When the internal gRPC hop supports Zstd, the frontend forwards
untrusted encoded uploads unchanged and returns the validating storage server's
result. A distinct upload API prevents unvalidated input from entering the trusted
download fast path. Storage decodes once and encodes the indexed frames; the
frontend uses no codecs. Identity-only hops and intermediate consumers that need
logical bytes retain the validating decode/encode fallback.

## Read behavior and fresh-store requirement

- Start with an empty store. Unindexed raw records larger than 4 KiB are rejected.
  Persistent stores write a durable `storage-format` marker under
  `persistent.stateDirectoryPath`. Startup refuses a different mode or version
  before opening the block device. Unmarked `state` or `state.new` files count
  as legacy raw state and cannot be opened with compression enabled.
  Tiny raw records produced by this mode receive bounded logical validation.
- Full compressed reads verify each stored frame's encoded checksum before
  exposing its bytes. Trusted backend streams retain their encoded representation
  at the frontend; the final client must validate the logical digest.
- Compressed offset reads pass their logical offset through the frontend to
  storage. Aligned reads forward stored frames unchanged; unaligned reads decode
  and re-encode only the first partial frame. The decoder is released before
  acquiring the encoder, and both are released before sending that frame.
- Stored compressed `ReadAt` and direct identity-offset reads skip preceding
  frames, including through metrics wrappers. Identity reads
  through a frontend still use its logical decoding fallback. Retrying reads also
  retain logical validation. Fallback encoding produces bounded independent frames
  and releases the encoder before sending bytes or reading more input, including
  retry input. Its input buffer is bounded by the expected logical size and 1 MiB.
- Authorization, metrics, and shard error annotation remain in the request path.
  Retrying decorators use the logical fallback: compressed byte offsets are not
  safely interchangeable between different encodings of the same blob.
- Block-device integrity-cache shortcuts are disabled in compressed mode. Encoded
  checksums and tiny-record logical validation remain enabled.
- Existing REAPI digest and ByteStream APIs are unchanged. No chunk-discovery or
  content-defined-chunking capability is advertised. Independent frame reads in
  this package are not a protocol for clients to discover parallel downloads.

This mode requires a fresh cache. There is no migration or read-compressed/write-raw
mode. Retire the cache data before changing back to raw-only storage or an
incompatible format. A recognized unsupported footer version returns
`FAILED_PRECONDITION` without invalidation. Changing shard configuration or
deploying this code is a separate operator action.

To start fresh, stop every process using the store, retain the old cache only as
an offline backup if needed, and configure a new empty `persistent.stateDirectoryPath`
and fresh key-location-map backing files. Provision fresh block backing storage
as well when retiring the old cache. A state directory must contain none of
`state`, `state.new`, or `storage-format`; do not remove only the format marker
from an existing store. For nonpersistent configurations, startup initializes a
new store. Older binaries do not understand the marker, so the guard is not a
safe rollback mechanism for them.

Unsupported-version records are not evidence of block corruption. A duplicate
upload checks the stored index; an unreadable record permits a validated repair
that replaces its mapping without evicting neighboring blocks, including with
hierarchical instance names. Presence checks alone may still report such a
record as present until a read or an explicit repair.

## Format integrity and bounds

`Encode` validates the complete logical digest before committing the index/footer.
Its caller must publish only after successful completion; a readable temporary
object alone does not establish success after cancellation or an I/O error.
`Open` checks the index against the expected logical digest. Checksums detect
accidental corruption, not hostile replacement of both content and metadata.
Only representations produced by successful streaming validation may be published.

The format supports logical frame sizes from 4 KiB to 4 MiB, at most 65,536 frames,
and a serialized index of at most 32 MiB. An empty blob has one valid empty frame.
Encoded size and parser entry counts are bounded independently. The whole logical
blob is never buffered. With the integrated 1 MiB frame size, the format supports
up to 64 GiB per logical blob; the separate CAS upload policy can impose a lower
limit. Indexed incompressible records may occupy slightly more space than raw
bytes. Built-in encoders use known-size encoding; a 1 MiB frame does
not require the default streaming encoder's 8 MiB history window. Keep the decoder
window limit at least 1 MiB for default integrated stored frames. Smaller configured
encoder windows are respected rather than enlarged by single-segment encoding. Custom streaming-only codec implementations retain their own frame settings.

`Reader` supports concurrent frame reads when its immutable backing `ReaderAt`
does. Its owner must keep that backing object alive through all readers. The local
buffer implementation pins block references across clones and refreshes. Opening
and validating the index is deferred until consumption, outside the local store
metadata lock; construction, size queries, and cloning do not read the device.
Each read factory has a 64 MiB LRU budget for verified indexes. Hits read the footer
but skip index I/O and JSON parsing; frame checksums are always verified. Entries
include content digest, physical size, and the footer checksum. They retain no
file handles, block references, or instance names. Eviction and oversized indexes
fall back to normal bounded parsing. Returned readers can keep an evicted index
alive until those requests finish; normal request admission must also bound active
read memory.
Device I/O errors, including `DATA_LOSS` and short reads, do not by themselves
prove corruption. Only verified format/checksum failures invalidate blocks.

## Encoded ingress and operational bounds

Compressed upload ingress is limited to logical size plus 12.5%, 1 KiB per logical
MiB (rounded up, minimum one), and 1 KiB fixed overhead. The frontend checks this
while forwarding, without acquiring codecs. Storage applies the same bound to
streaming input, including compressed replication streams. Excess input returns
`INVALID_ARGUMENT`, including oversized duplicates. This is an explicit upload
policy: otherwise legal Zstandard streams containing excessive empty or skippable
frames are rejected. Existing logical blob-size limits remain independent.

Slow clients can still fill the finite receive queue or staging budget. Configure
RPC admission/deadlines and size staging for the intended concurrent workload;
overload remains a bounded rejection, not unlimited buffering. Only the validated
output representation is staged before publication. Encoder-pool size alone does
not bound transport memory. Read-side logical consumers and synchronized logical
replication clones may still decode network streams as they consume them; codecs
already acquired by an upstream consumer cannot be moved to the upload pool.

## Verification

`//pkg/blobstore/compressionintegration:compressionintegration_test` runs the real
configuration builder, file-backed local store, CAS client, ByteStream handlers,
authorization, metrics, and sharding over in-process gRPC connections. It checks:

- A logical blob larger than a block fits through compressed physical allocation.
- Full compressed reads return exactly the stored frames with zero codec leases.
- Flat and hierarchical block refreshes preserve those bytes and avoid codecs.
- Compressed uploads use no frontend codecs; storage validates and frames once.
- Duplicate compressed uploads drain without codec acquisition at storage and
  share the bounded receive queue.
- Aligned compressed offsets use no codecs; partial first frames use one decoder
  and encoder at storage and none at the frontend. Identity reads remain correct.
- Truncated uploads, digest/size mismatches, authorization failures, and upload
  limits are enforced with compressed and identity-only internal hops.
- Source failures cancel raw-backend codec waits promptly. Early backend rejection
  interrupts paused upload reads and frontend codec waits, even before the client
  supplies bytes or closes its send side. Both identity and compressed forwarding
  preserve source-close and background-task errors.
- A blocked index read permits unrelated writes to complete. Lazy clones retain
  their record until the final consumer releases it.
- Known backend acquisition errors propagate before waiting for decoder capacity.
- Local duplicate uploads retain source-close and background-task errors,
  including errors carried through opaque reader/chunk adapters.
- Index/frame I/O failures propagate without corrupt-block eviction. Unsupported
  versions do not evict records. Isolated metadata mutations cover digest identity,
  digest function, physical gaps, and decoded frame hashes.
- Metrics-wrapped random and offset reads touch only the requested frames.
  Resolved tiny-record clones can be consumed sequentially, and cloned streams retain
  terminal close errors. Physical refresh buffers reject alternate data access.
- Denied instances, corrupt frames/indexes, and invalid uploads fail correctly.
- Retrying a disconnected source resumes at the logical offset with another
  compressed encoding, without holding the encoder during source replacement.
- Failed uploads release staging resources; copying to raw storage stays logical.
- Stalled metrics-wrapped uploads use the isolated upload pool; a one-decoder
  read pool still serves reads. Identity writes can use available upload encoders.
- A blocked later input frame does not prevent encoding an earlier frame, and
  uploads succeed with only the output reservation available.
- Incompatible upload/read codec windows fail before the storage device opens.
  Recursive same-destination repair succeeds with sufficient staging headroom.
- Staging contention waits, honors cancellation, and admits work after release.
- Malformed primary streams retry from a complete replacement encoding, retaining
  backend integrity attribution and source lifetime through validation.
- Excess encoded padding is rejected without codecs at the frontend and on
  duplicate uploads. Exact-budget streams and terminal source errors are covered.
- Index-cache hits use the current backing reader, verify frame corruption, retain
  the caller's instance only for its active reader, and obey LRU eviction.
- Explicit truncated-stream errors and errors accompanying a full chunk remain
  failures; storage `OutOfRange` errors are not interpreted as frame exhaustion.
- Fallback replication releases the failed decoder before acquiring its
  replacement, including streamed identity and compressed reads.

Format tests additionally cover reopened files, concurrent frame reads, malformed
metadata, truncation, cancellation, and source/destination errors. Mixed compressed
read/write and copy regressions use a shared one-encoder/one-decoder pool.
These tests establish correctness in the exercised paths, not throughput under a
particular deployment's workload. Load comparison and worker-specific end-to-end
validation are still needed before broad rollout.

A separate local smoke test with Bazel 9.3.1 and two `bb_storage` processes
uploaded a 5.1 MB artifact and obtained a correct remote-cache hit. Server metrics
confirmed `ByteStream.Read` with no `BatchReadBlobs` call; the artifact exceeded
the configured 4 MiB batch size. The same probe rejected an oversized cache
upload without failing the local build or publishing an incomplete Action Cache
entry. This does not establish compatibility with every remote-execution worker.

The current mode requires a fresh store; old raw-record migration probes are not
compatibility guarantees. Graceful-restart and abrupt-power-loss behavior need
separate durability validation.
