package zstdframes

import (
	"container/list"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type indexCacheKey struct {
	digest digest.Digest
	size   int64
	footer [footerSize]byte
}

type cachedIndex struct {
	key    indexCacheKey
	reader Reader
	charge int64
}

// IndexCache retains verified immutable indexes, never backing readers or block
// references. Every lookup rereads the footer, and every frame read still checks
// the stored bytes. The checksum key allows block refreshes and replacement
// encodings without trusting stale location information.
type IndexCache struct {
	mu      sync.Mutex
	maximum int64
	used    int64
	entries map[indexCacheKey]*list.Element
	order   list.List
}

func NewIndexCache(maximumBytes int64) *IndexCache {
	return &IndexCache{maximum: maximumBytes, entries: map[indexCacheKey]*list.Element{}}
}

func (c *IndexCache) Open(source io.ReaderAt, size int64, expected digest.Digest) (*Reader, error) {
	if expected == digest.BadDigest || size < footerSize {
		return nil, ErrInvalidRepresentation
	}
	// Instance names do not affect the representation and may be arbitrarily
	// large. Construct an owned, instance-free digest rather than retaining an
	// unaccounted instance string (or a substring of its backing allocation).
	identity := digest.MustNewDigest("", expected.GetDigestFunction().GetEnumValue(), expected.GetHashString(), expected.GetSizeBytes())
	key := indexCacheKey{digest: identity, size: size}
	if err := readAtFull(source, key.footer[:], size-footerSize); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if e := c.entries[key]; e != nil {
		c.order.MoveToFront(e)
		r := e.Value.(*cachedIndex).reader
		c.mu.Unlock()
		r.source = source
		r.digest = expected
		return &r, nil
	}
	c.mu.Unlock()
	r, err := openIndex(source, size, expected, key.footer)
	if err != nil {
		return nil, err
	}
	// Account for serialized metadata, parsed strings, allocated frame capacity,
	// and fixed map/list/key overhead. Oversized entries simply bypass the cache.
	charge := int64(1024 + cap(r.metadata) + cap(r.index.Frames)*128 + len(r.index.Hash))
	for _, f := range r.index.Frames {
		charge += int64(len(f.Hash) + len(f.EncodedHash))
	}
	if charge <= c.maximum {
		copy := *r
		copy.source = nil
		copy.digest = identity
		c.mu.Lock()
		if c.entries[key] == nil {
			for c.used > c.maximum-charge {
				e := c.order.Back()
				old := e.Value.(*cachedIndex)
				delete(c.entries, old.key)
				c.used -= old.charge
				c.order.Remove(e)
			}
			c.entries[key] = c.order.PushFront(&cachedIndex{key: key, reader: copy, charge: charge})
			c.used += charge
		}
		c.mu.Unlock()
	}
	return r, nil
}
