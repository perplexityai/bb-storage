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

New uploads reserve one write slot and a conservative bound on staging bytes:
encoded expansion plus metadata proportional to frame count. Empty blobs reserve
about 2 KiB rather than the maximum index budget. Configuration too small for one
record is rejected before opening the device. A blob that can never fit returns
`INVALID_ARGUMENT`; temporary slot or byte-budget contention returns
`RESOURCE_EXHAUSTED`. Admission remains fail-fast, with no unbounded waiting queue.
An unlinked temporary file receives the representation. Validation must finish
before local allocation publishes it. A crash between file creation and unlink
can leave an empty named file; payload writes begin only after unlink succeeds.

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

## Compatibility and fallback behavior

- Existing raw records remain readable and receive normal logical validation.
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
  checksums and legacy logical validation remain enabled.
- Existing REAPI digest and ByteStream APIs are unchanged. No chunk-discovery or
  content-defined-chunking capability is advertised. Independent frame reads in
  this package are not a protocol for clients to discover parallel downloads.

This is a storage-format rollout. **Older raw-only readers cannot read compressed
records.** Keep a compatible reader and `casCompression` configured after enabling
writes, or retire the affected cache data before reverting to an older binary.
Disabling the option is not a safe rollback of a populated compressed store:
raw-only validation may invalidate the containing block and all older blocks,
losing unrelated cache records. A recognized newer footer version returns
`FAILED_PRECONDITION` without invalidation. A read-compressed/write-raw migration
mode is not implemented; stopping compressed writes alone would not make existing
compressed records readable by an older binary.
Changing shard configuration or deploying this code is a separate operator action.

## Format integrity and bounds

`Encode` validates the complete logical digest before committing the index/footer.
Its caller must publish only after successful completion; a readable temporary
object alone does not establish success after cancellation or an I/O error.
`Open` checks the index against the expected logical digest. Checksums detect
accidental corruption, not hostile replacement of both content and metadata.
Only representations produced by successful local validation may be published.

The format supports logical frame sizes from 4 KiB to 4 MiB, at most 65,536 frames,
and a serialized index of at most 32 MiB. An empty blob has one valid empty frame.
Encoded size and parser entry counts are bounded independently. The whole logical
blob is never buffered. With the integrated 1 MiB frame size, the format supports
up to 64 GiB per logical blob; the separate CAS upload policy can impose a lower
limit. Indexed incompressible records may occupy slightly more space than raw
bytes. Built-in encoders use known-size encoding; a 1 MiB frame does
not require the default streaming encoder's 8 MiB history window. Keep the decoder
window limit at least 1 MiB for default integrated stored frames. Smaller configured
encoder windows are respected rather than enlarged by single-segment encoding. Previously written
streaming frames may require a larger window until those records have aged out.
Custom streaming-only codec implementations retain their own frame settings.

`Reader` supports concurrent frame reads when its immutable backing `ReaderAt`
does. Its owner must keep that backing object alive through all readers. The local
buffer implementation pins block references across clones and refreshes. Opening
and validating the index is deferred until consumption, outside the local store
metadata lock; construction, size queries, and cloning do not read the device.
Device I/O errors, including `DATA_LOSS` and short reads, do not by themselves
prove corruption. Only verified format/checksum failures invalidate blocks.

## Remaining operational limits

A write slot spans the upload, and a compressed upload still owns its decoder
while receiving client bytes. Slow clients can exhaust write slots; reserve
read-side decoder headroom above concurrent uploads and configure request
admission and deadlines. Encoder-pool size alone does not bound buffered transport
memory. Separating upload decoder capacity or staging encoded input before decoding
requires a distinct resource policy and workload measurements.

Legacy raw records still receive full logical validation on partial reads;
`dataIntegrityValidationCache` is deliberately bypassed in compressed mode. Large
legacy records can therefore be expensive for worker-local random reads during
migration. Compressed records use the indexed path. The JSON index is parsed per
read; maximum-format-size workloads still need memory and latency measurements.

Logical upload limits do not cap encoded byte volume. Zstandard permits legal
empty and skippable frames, so a digest-derived encoded-size cap would be an
additional accepted-upload policy. No such cap is imposed here. It must be chosen
and tested explicitly before exposing this path to untrusted or unbounded senders.

## Verification

`//pkg/blobstore/compressionintegration:compressionintegration_test` runs the real
configuration builder, file-backed local store, CAS client, ByteStream handlers,
authorization, metrics, and sharding over in-process gRPC connections. It checks:

- A logical blob larger than a block fits through compressed physical allocation.
- Full compressed reads return exactly the stored frames with zero codec leases.
- Flat and hierarchical block refreshes preserve those bytes and avoid codecs.
- Compressed uploads use no frontend codecs; storage validates and frames once.
- Legacy compressed clients finish duplicate uploads without codec acquisition
  at storage. Duplicate streams are drained for compatibility.
- Aligned compressed offsets use no codecs; partial first frames use one decoder
  and encoder at storage and none at the frontend. Identity reads remain correct.
- Truncated uploads, digest/size mismatches, authorization failures, and upload
  limits are enforced with compressed and identity-only internal hops.
- Source failures cancel backend codec waits promptly. Early backend rejection
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
  Resolved legacy clones can be consumed sequentially, and cloned streams retain
  terminal close errors. Physical refresh buffers reject alternate data access.
- Denied instances, corrupt frames/indexes, and invalid uploads fail correctly.
- Retrying a disconnected source resumes at the logical offset with another
  compressed encoding, without holding the encoder during source replacement.
- Failed uploads release staging resources; copying to raw storage stays logical.
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

A persistent-daemon restart check also verifies mixed raw and compressed records:
write raw data, restart with compression enabled, write compressed data, then
restart again and reconstruct both through identity and compressed reads. This
covers graceful restart, not abrupt-power-loss durability.
