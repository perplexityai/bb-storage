// Package zstdframes stores a logical CAS blob as independently compressed
// Zstandard frames. Its index permits verified compressed reads without a
// decoder, and independent frame reads without a shared stream cursor.
package zstdframes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	minimumFrameSize = 4 << 10
	maximumFrameSize = 4 << 20
	maximumFrames    = 1 << 16
	maximumIndexSize = 32 << 20
	footerSize       = 48
	magic            = "BBZSTF01"
)

type frame struct {
	Hash        string `json:"hash"`
	Size        int64  `json:"size"`
	Offset      int64  `json:"offset"`
	EncodedSize int64  `json:"encodedSize"`
	EncodedHash string `json:"encodedHash"`
}

type index struct {
	Hash      string                               `json:"hash"`
	Size      int64                                `json:"size"`
	Function  remoteexecution.DigestFunction_Value `json:"function"`
	FrameSize int                                  `json:"frameSize"`
	Frames    frameList                            `json:"frames"`
}

type frameList []frame

func (frames *frameList) UnmarshalJSON(data []byte) error {
	// Checking the count only after json.Unmarshal permits a small JSON array
	// of empty objects to allocate many more entries than the format allows.
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token == nil {
		*frames = nil
		return nil
	}
	if token != json.Delim('[') {
		return status.Error(codes.DataLoss, "Frame index is not an array")
	}
	*frames = nil
	for d.More() {
		if len(*frames) == maximumFrames {
			return status.Error(codes.DataLoss, "Frame index contains too many entries")
		}
		var f frame
		if err := d.Decode(&f); err != nil {
			return err
		}
		*frames = append(*frames, f)
	}
	_, err = d.Token()
	return err
}

// Reader owns no file descriptor. The caller must keep its immutable backing
// object open until all reads finish. Concurrent reads are supported when the
// underlying ReaderAt supports them, as os.File does.
type Reader struct {
	source io.ReaderAt
	digest digest.Digest
	index  index
}

func encodedLimit(frameSize int) int64 {
	return int64(frameSize + frameSize/8 + 1024)
}

func checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

type boundedWriter struct {
	bytes.Buffer
	limit int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-int64(w.Len()) {
		return 0, status.Error(codes.ResourceExhausted, "Compressed frame exceeds its size budget")
	}
	return w.Buffer.Write(p)
}

func writeAll(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return err
}

// Encode writes a versioned representation, validating the entire logical
// digest before writing its index and commit footer. Callers must write to a
// temporary object and publish it only after success; failures leave partial
// output. The caller owns both streams and is responsible for interrupting
// blocked I/O on cancellation. Working memory is bounded by a frame, a codec,
// and at most 65536 index entries, independent of the logical blob size.
func Encode(ctx context.Context, dst io.Writer, d digest.Digest, src io.Reader, frameSize int, pool zstd.Pool) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if d == digest.BadDigest || frameSize < minimumFrameSize || frameSize > maximumFrameSize || pool == nil {
		return status.Error(codes.InvalidArgument, "Invalid digest, frame size, or codec pool")
	}
	size := d.GetSizeBytes()
	if size > int64(frameSize)*maximumFrames {
		return status.Error(codes.ResourceExhausted, "Blob requires too many frames")
	}
	idx := index{Hash: d.GetHashString(), Size: size, Function: d.GetDigestFunction().GetEnumValue(), FrameSize: frameSize}
	whole := d.GetDigestFunction().NewGenerator(size)
	data := make([]byte, frameSize)
	var offset int64
	for remaining := size; remaining > 0; {
		if err := checkContext(ctx); err != nil {
			return err
		}
		n := min(int64(frameSize), remaining)
		p := data[:int(n)]
		if _, err := io.ReadFull(src, p); err != nil {
			return err
		}
		whole.Write(p)
		part := d.GetDigestFunction().NewGenerator(n)
		part.Write(p)
		encoded := boundedWriter{limit: encodedLimit(frameSize)}
		encoder, err := pool.NewEncoder(ctx, &encoded)
		if err != nil {
			return err
		}
		writeErr := writeAll(encoder, p)
		closeErr := encoder.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := writeAll(dst, encoded.Bytes()); err != nil {
			return err
		}
		h := sha256.Sum256(encoded.Bytes())
		idx.Frames = append(idx.Frames, frame{Hash: part.Sum().GetHashString(), Size: n, Offset: offset, EncodedSize: int64(encoded.Len()), EncodedHash: hex.EncodeToString(h[:])})
		offset += int64(encoded.Len())
		remaining -= n
	}
	// Reading through EOF detects extra data and deferred source errors.
	var extra [1]byte
	if n, err := io.ReadFull(src, extra[:]); n != 0 || err != io.EOF {
		if err != nil && err != io.EOF {
			return err
		}
		return status.Error(codes.InvalidArgument, "Blob exceeds its declared size")
	}
	if whole.Sum() != d {
		return status.Error(codes.InvalidArgument, "Blob digest mismatch")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	metadata, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	if len(metadata) > maximumIndexSize {
		return status.Error(codes.ResourceExhausted, "Frame index exceeds its size budget")
	}
	if err := writeAll(dst, metadata); err != nil {
		return err
	}
	var footer [footerSize]byte
	copy(footer[:8], magic)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(metadata)))
	h := sha256.Sum256(metadata)
	copy(footer[16:], h[:])
	return writeAll(dst, footer[:])
}

// Open checks the bounded index and its checksum against the expected logical
// digest. Frame checksums are checked before their bytes are returned. This
// detects storage corruption; it is not authentication of untrusted uploads.
// Only representations produced by Encode may be published in this store.
func Open(source io.ReaderAt, size int64, expected digest.Digest) (*Reader, error) {
	bad := func() (*Reader, error) {
		return nil, status.Error(codes.DataLoss, "Invalid compressed representation index")
	}
	if expected == digest.BadDigest || size < footerSize {
		return bad()
	}
	var footer [footerSize]byte
	if _, err := source.ReadAt(footer[:], size-footerSize); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint64(footer[8:16])
	if string(footer[:8]) != magic || length > maximumIndexSize || length > uint64(size-footerSize) {
		return bad()
	}
	metadata := make([]byte, int(length))
	end := size - footerSize - int64(length)
	if _, err := source.ReadAt(metadata, end); err != nil {
		return nil, err
	}
	h := sha256.Sum256(metadata)
	if !bytes.Equal(h[:], footer[16:]) {
		return bad()
	}
	var idx index
	if err := json.Unmarshal(metadata, &idx); err != nil {
		return bad()
	}
	if idx.Hash != expected.GetHashString() || idx.Size != expected.GetSizeBytes() || idx.Function != expected.GetDigestFunction().GetEnumValue() || idx.FrameSize < minimumFrameSize || idx.FrameSize > maximumFrameSize || len(idx.Frames) > maximumFrames {
		return bad()
	}
	remaining := idx.Size
	var offset int64
	for _, f := range idx.Frames {
		if remaining == 0 || f.Size != min(int64(idx.FrameSize), remaining) || f.Offset != offset || f.EncodedSize <= 0 || f.EncodedSize > encodedLimit(idx.FrameSize) || f.EncodedSize > end-offset {
			return bad()
		}
		if _, err := expected.GetDigestFunction().NewDigest(f.Hash, f.Size); err != nil {
			return bad()
		}
		if b, err := hex.DecodeString(f.EncodedHash); err != nil || len(b) != sha256.Size {
			return bad()
		}
		remaining -= f.Size
		offset += f.EncodedSize
	}
	if remaining != 0 || offset != end {
		return bad()
	}
	return &Reader{source: source, digest: expected, index: idx}, nil
}

// FrameDigests returns a copy of the ordered logical chunk mapping. The
// mapping describes fixed-size frames, not a content-defined chunking scheme.
func (r *Reader) FrameDigests() []digest.Digest {
	digests := make([]digest.Digest, len(r.index.Frames))
	for i, f := range r.index.Frames {
		digests[i], _ = r.digest.GetDigestFunction().NewDigest(f.Hash, f.Size)
	}
	return digests
}

// ReadCompressedFrame returns one independent Zstandard frame after checking
// its encoded checksum. It acquires no codec and shares no read cursor with
// other calls, so callers may fetch frames in parallel with bounded workers.
func (r *Reader) ReadCompressedFrame(ctx context.Context, i int) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if i < 0 || i >= len(r.index.Frames) {
		return nil, status.Error(codes.OutOfRange, "Frame index is out of range")
	}
	f := r.index.Frames[i]
	data := make([]byte, int(f.EncodedSize))
	if _, err := r.source.ReadAt(data, f.Offset); err != nil {
		return nil, err
	}
	h := sha256.Sum256(data)
	if hex.EncodeToString(h[:]) != f.EncodedHash {
		return nil, status.Error(codes.DataLoss, "Compressed frame checksum mismatch")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return data, nil
}

// WriteCompressed concatenates verified frames into a standard Zstandard
// stream, without encoding or decoding. The index and footer are not sent.
func (r *Reader) WriteCompressed(ctx context.Context, dst io.Writer) error {
	for i := range r.index.Frames {
		data, err := r.ReadCompressedFrame(ctx, i)
		if err != nil {
			return err
		}
		if err := writeAll(dst, data); err != nil {
			return err
		}
	}
	return checkContext(ctx)
}

// WriteIdentity decodes only frames overlapping the requested logical suffix.
// A decoder is released before writing to the consumer, so slow readers never
// hold a codec while waiting for network capacity.
func (r *Reader) WriteIdentity(ctx context.Context, dst io.Writer, offset int64, pool zstd.Pool) error {
	if offset < 0 || offset > r.digest.GetSizeBytes() || pool == nil {
		return status.Error(codes.InvalidArgument, "Invalid logical offset or codec pool")
	}
	if offset == r.digest.GetSizeBytes() {
		return checkContext(ctx)
	}
	i := int(offset / int64(r.index.FrameSize))
	skip := offset % int64(r.index.FrameSize)
	for ; i < len(r.index.Frames); i++ {
		encoded, err := r.ReadCompressedFrame(ctx, i)
		if err != nil {
			return err
		}
		f := r.index.Frames[i]
		decoder, err := pool.NewDecoder(ctx, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(decoder, f.Size+1))
		decoder.Close()
		if err != nil {
			return err
		}
		g := r.digest.GetDigestFunction().NewGenerator(f.Size)
		g.Write(data)
		if int64(len(data)) != f.Size || g.Sum().GetHashString() != f.Hash {
			return status.Error(codes.DataLoss, "Decoded frame digest mismatch")
		}
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := writeAll(dst, data[skip:]); err != nil {
			return err
		}
		skip = 0
	}
	return checkContext(ctx)
}
