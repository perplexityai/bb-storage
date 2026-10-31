# Indexed Zstandard representation experiment

This package is an isolated storage-format experiment, not an enabled CAS backend.
It tests whether a logical blob can remain compressed on disk and across transport hops while supporting independent parallel frame reads.
No server configuration, existing storage format, or capability advertisement changes.

```text
logical CAS digest (hash and size of uncompressed bytes)
  -> independently encoded Zstandard frames
  -> bounded index with ordered logical digests and encoded checksums
  -> versioned commit footer
```

`Encode` verifies the complete logical digest before writing the index and footer.
The caller must use a temporary object, discard it on error, and publish it atomically only after successful completion and any required durability operations.
Cancellation or a destination error can occur after the footer bytes have been written; a readable temporary object is not a substitute for a successful `Encode` result.
The package neither owns files nor implements publication, eviction, disk quotas, or concurrent fill admission.

`Open` validates the index against the expected logical digest.
`ReadCompressedFrame` verifies an encoded checksum before returning any bytes from that frame.
`WriteCompressed` concatenates the verified frames without acquiring an encoder or decoder.
`WriteIdentity` decodes only frames overlapping a logical suffix, validates each logical chunk, and releases its decoder before writing to a potentially slow consumer.
Backing objects must remain immutable and readable until all readers finish.

Checksums detect accidental corruption; they do not authenticate an arbitrary uploaded index.
Only locally generated representations from successful `Encode` calls may be published.
Client-supplied indexes must never bypass logical content validation.
An index associates logical chunks with compressed checksums based on that validation; compressed reads do not repeat logical validation by decoding.

The configured logical frame size ranges from 4 KiB to 4 MiB; the last frame may be smaller, and an empty blob has one valid empty Zstandard frame.
There are at most 65,536 frames and a 32 MiB serialized index.
Encoded frame size is bounded separately, and the index parser bounds entry count before materializing the complete array.
The entire logical blob is never buffered.
These are format resource bounds, not the configurable CAS upload policy.
Readers and writers retain ownership of their I/O handles and must interrupt blocked I/O when their context is canceled.

## What the tests establish

- Reopened file-backed representations support compressed reads and concurrent independent frame reads.
- Concatenated stored frames decode to the original bytes and digest using the existing Go Zstandard implementation.
- Read-only gRPC fixtures forward the exact stored bytes across two hops; parallel chunk requests reconstruct the logical blob.
- Identity reads handle offsets without decoding earlier frames.
- Truncated, oversized, or incorrectly hashed uploads never produce a readable completed representation.
- Corrupt frames are rejected before their bytes are returned; invalid index lengths, ranges, counts, and digests are rejected.
- Cancellation and source, destination, and transport errors propagate.
- Errors returned alongside final source bytes remain errors; full `ReaderAt` reads may return `EOF` without failing the transfer.
- Separate fuzz targets exercise raw representations and mutated index metadata with recomputed checksums, so index parsing is not hidden behind the checksum gate.

The transport fixtures are deliberately separate from the production server.
They do not establish that its authorization, replication, retry, admission, or metrics decorators preserve this representation.
They also do not establish compatibility with every Bazel or remote-execution worker decoder.

## Integration gates

1. Choose the persistent representation owner and implement atomic publication, disk accounting, bounded single-flight fills, and eviction while readers are active.
2. Carry encoded representations through the existing storage and frontend paths without bypassing authorization or acquiring codecs on full compressed reads.
   Preserve decoder-before-encoder acquisition on fallbacks, cancellation, final-send failures, and retry semantics.
3. Expose ordered chunk mappings and independently addressable chunks using the supported remote-execution protocol, with coherent retention and missing-chunk recovery.
   Fixed-size frames here are not an implementation of FastCDC or RepMaxCDC and must not be advertised as either algorithm.
4. Verify actual Bazel and worker clients, identity and compressed offsets, corruption, shard loss, mixed reads/writes, and bounded failure recovery.
5. Compare CPU, memory, disk footprint, network traffic, and latency under equivalent workloads before selecting the format for rollout.

This experimental format may change before integration.
Existing uncompressed files must not be reinterpreted as this format, and no migration is part of this change.
