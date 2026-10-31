# Compressed primary CAS storage

Local block-device CAS storage can retain validated Zstandard representations as
its primary records. Full compressed ByteStream reads forward the stored frame
bytes through storage and frontend servers without acquiring encoders or decoders.
The logical CAS digest remains the hash and size of the uncompressed bytes.

```text
upload -> validate logical bytes -> stage frames + index -> publish local record
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

Before consuming an upload, the storage wrapper reserves one write slot and a
conservative bound on staging bytes: worst-case encoded frame expansion plus a
32 MiB index allowance. Excess work fails with `RESOURCE_EXHAUSTED`. An unlinked
temporary file receives the representation; errors, cancellation, and process
exit reclaim it. Validation must finish successfully before the existing local
allocator publishes the record. The allocator, key-location map, persistence,
eviction, and reader references remain responsible for the primary store.
Allocation and local refresh use the physical record length; keys and API sizes
remain logical. Refresh preserves the compressed representation without codecs.
Ordinary copies into another backend use logical bytes and its own storage format.

New records use independent 1 MiB logical frames, followed by a bounded index and
versioned footer. This is the primary copy, not a second read cache. Uploads are
validated and encoded at storage; arbitrary client-supplied encoded indexes are
never accepted. Compressed upload hops may still decode and re-encode. The
zero-codec path applies to full compressed **reads**.

## Compatibility and fallback behavior

- Existing raw records remain readable and receive normal logical validation.
- Full compressed reads verify each stored frame's encoded checksum before
  exposing its bytes. Trusted backend streams retain their encoded representation
  at the frontend; the final client must validate the logical digest.
- Identity reads and compressed reads with a nonzero logical offset decode as
  needed. Fallback encoding produces bounded independent frames and releases the
  encoder before reading more input, including retry input. This avoids holding an
  encoder while a shared bounded pool is needed for a decoder.
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
Disabling the option is not a safe rollback of a populated compressed store.
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
limit. Incompressible and tiny records may occupy more space than raw data.

`Reader` supports concurrent frame reads when its immutable backing `ReaderAt`
does. Its owner must keep that backing object alive through all readers. The local
buffer implementation pins block references across clones and refreshes.

## Verification

`//pkg/blobstore/compressionintegration:compressionintegration_test` runs the real
configuration builder, file-backed local store, CAS client, ByteStream handlers,
authorization, metrics, and sharding over in-process gRPC connections. It checks:

- A logical blob larger than a block fits through compressed physical allocation.
- Full compressed reads return exactly the stored frames with zero codec leases.
- Flat and hierarchical block refreshes preserve those bytes and avoid codecs.
- Identity and offset reads reconstruct the expected bytes.
- Denied instances, corrupt frames/indexes, and invalid uploads fail correctly.
- Retrying a disconnected source resumes at the logical offset with another
  compressed encoding, without holding the encoder during source replacement.
- Failed uploads release staging resources; copying to raw storage stays logical.

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
