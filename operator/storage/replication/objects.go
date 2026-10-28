package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	vtv1 "pkg.akt.dev/go/volume/v1"

	"github.com/akash-network/provider/bucket"
)

// ObjectSender is the send half of the ExportObjects stream
// (vtv1.VolumeTransfer_ExportObjectsServer satisfies it).
type ObjectSender interface {
	Send(*vtv1.ObjectChunk) error
}

// SendObjects streams every object of a bucket whose key sorts after
// startAfter, in key order, each in chunks of up to chunkSize bytes: its
// first chunk carries its attributes, its last the sha256 of the whole
// object. It returns the objects and bytes sent.
func SendObjects(ctx context.Context, sender ObjectSender, objects bucket.Objects, startAfter string, chunkSize int) (int, uint64, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	buf := make([]byte, chunkSize)

	var (
		count int
		total uint64
	)

	for after := startAfter; ; {
		page, more, err := objects.List(ctx, after, 0)
		if err != nil {
			return count, total, err
		}

		for _, listed := range page {
			size, err := sendObject(ctx, sender, objects, listed.Key, buf)
			if err != nil {
				return count, total, err
			}

			count++
			total += size
			after = listed.Key
		}

		if !more || len(page) == 0 {
			return count, total, nil
		}
	}
}

func sendObject(ctx context.Context, sender ObjectSender, objects bucket.Objects, key string, buf []byte) (uint64, error) {
	info, body, err := objects.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = body.Close()
	}()

	if info.Size < 0 {
		return 0, fmt.Errorf("%w: object %q has a negative size", ErrReplication, key)
	}

	attributes := &vtv1.ObjectAttributes{
		Size_:              uint64(info.Size),
		ContentType:        info.ContentType,
		ContentEncoding:    info.ContentEncoding,
		ContentDisposition: info.ContentDisposition,
		ContentLanguage:    info.ContentLanguage,
		CacheControl:       info.CacheControl,
		Expires:            info.Expires,
		Metadata:           info.Metadata,
	}

	whole := sha256.New()

	for offset, remaining := int64(0), info.Size; ; {
		n := min(int64(len(buf)), remaining)
		if _, err := io.ReadFull(body, buf[:n]); err != nil {
			return 0, fmt.Errorf("%w: object %q ended before its %d bytes: %s", ErrReplication, key, info.Size, err.Error())
		}

		data := append([]byte(nil), buf[:n]...)
		sum := sha256.Sum256(data)
		whole.Write(data)

		chunk := &vtv1.ObjectChunk{Key: key, Offset: uint64(offset), Data: data, SHA256: sum[:]} // nolint: gosec
		if offset == 0 {
			chunk.Attributes = attributes
		}

		offset += n
		remaining -= n

		if remaining == 0 {
			chunk.ObjectSHA256 = whole.Sum(nil)
		}

		if err := sender.Send(chunk); err != nil {
			return 0, err
		}

		if remaining == 0 {
			return uint64(info.Size), nil
		}

		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
}

// BucketSyncer copies the objects of a bucket that moved here from the
// provider it moved from, into this provider's bucket. Each object lands
// whole: spooled, checked chunk by chunk and against the sha256 of the
// whole object, then written. A broken stream resumes after the last object
// that landed.
type BucketSyncer struct {
	// Client is the dialed source provider (mTLS with x/cert identities).
	Client vtv1.VolumeTransferClient
	// Open opens this provider's bucket the objects land in.
	Open func(ctx context.Context) (bucket.Objects, error)
	// Owner, Vid and DSeq identify the bucket on the source's side.
	Owner string
	Vid   string
	DSeq  uint64
	// Requester is this (the destination) provider's address; it must
	// match our client certificate.
	Requester string
	// SpoolDir holds the object in flight; empty uses the OS temp dir.
	SpoolDir string
	// MaxBytes bounds what the source may send in all; zero leaves it
	// unbounded. A source is not trusted to stay within the bucket it
	// leased, and each object is spooled on this provider's disk.
	MaxBytes uint64

	// after is the key of the last object that landed
	after string
	// cleared records that the bucket no longer holds what it held before
	// the copy began
	cleared bool
	objects int
	bytes   uint64
}

// Objects and Bytes report what landed so far.
func (s *BucketSyncer) Objects() int {
	return s.objects
}

func (s *BucketSyncer) Bytes() uint64 {
	return s.bytes
}

// SyncOnce copies from where the last round stopped to the end of the
// bucket. complete reports that the bucket here holds every object of the
// source's final state and nothing else. A source whose bucket can still
// change is not an error: nothing lands, and the next round asks again.
func (s *BucketSyncer) SyncOnce(ctx context.Context) (bool, error) {
	objects, err := s.Open(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = objects.Close()
	}()

	for {
		progress, err := s.stream(ctx, objects)

		switch {
		case err == nil:
			return true, nil
		case IsNotFinal(err):
			return false, nil
		case !progress, ctx.Err() != nil, errors.Is(err, ErrChecksumMismatch), errors.Is(err, ErrTooLarge):
			return false, err
		}

		// the stream broke after objects landed: resume after the last
	}
}

// stream runs one ExportObjects stream to its end. progress reports that
// at least one object landed.
func (s *BucketSyncer) stream(ctx context.Context, objects bucket.Objects) (progress bool, err error) {
	stream, err := s.Client.ExportObjects(ctx, &vtv1.ExportObjectsRequest{
		Owner:      s.Owner,
		Vid:        s.Vid,
		DSeq:       s.DSeq,
		Requester:  s.Requester,
		StartAfter: s.after,
	})
	if err != nil {
		return false, err
	}

	var incoming *spooledObject

	defer func() {
		incoming.discard()
	}()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if incoming != nil {
				return progress, fmt.Errorf("%w: the stream ended inside an object", ErrReplication)
			}

			// a source with no objects sends none
			return progress, s.clear(ctx, objects)
		}
		if err != nil {
			return progress, err
		}

		// the source is final and serving: what the bucket here held
		// before, from an earlier lease, gives way to it
		if err := s.clear(ctx, objects); err != nil {
			return progress, err
		}

		if incoming == nil {
			if incoming, err = s.begin(chunk); err != nil {
				return progress, err
			}
		} else if chunk.Key != incoming.info.Key || chunk.Attributes != nil {
			return progress, fmt.Errorf("%w: an object started before the one in flight ended", ErrChecksumMismatch)
		}

		if err := incoming.write(chunk); err != nil {
			return progress, err
		}

		if len(chunk.ObjectSHA256) == 0 {
			continue
		}

		if err := incoming.finish(chunk.ObjectSHA256); err != nil {
			return progress, err
		}

		if err := objects.Put(ctx, incoming.info, incoming.file, incoming.sum); err != nil {
			return progress, err
		}

		s.after = incoming.info.Key
		s.objects++
		s.bytes += uint64(incoming.info.Size) // nolint: gosec
		progress = true

		incoming.discard()
		incoming = nil
	}
}

// clear empties the bucket here, once, before the first object of the
// copy lands.
func (s *BucketSyncer) clear(ctx context.Context, objects bucket.Objects) error {
	if s.cleared {
		return nil
	}

	for {
		page, _, err := objects.List(ctx, "", 0)
		if err != nil {
			return err
		}

		if len(page) == 0 {
			s.cleared = true
			return nil
		}

		for _, object := range page {
			if err := objects.Delete(ctx, object.Key); err != nil {
				return err
			}
		}
	}
}

// begin starts spooling the object chunk opens.
func (s *BucketSyncer) begin(chunk *vtv1.ObjectChunk) (*spooledObject, error) {
	attributes := chunk.Attributes
	if attributes == nil || chunk.Offset != 0 {
		return nil, fmt.Errorf("%w: an object starts without its attributes", ErrChecksumMismatch)
	}

	// keys only grow, so the stream never lands an object twice or skips
	// back over one
	if chunk.Key == "" || chunk.Key <= s.after {
		return nil, fmt.Errorf("%w: object keys out of order", ErrChecksumMismatch)
	}

	if s.MaxBytes > 0 && (attributes.Size_ > s.MaxBytes || s.bytes+attributes.Size_ > s.MaxBytes) {
		return nil, fmt.Errorf("%w: the source's objects pass the %d bytes this bucket allows", ErrTooLarge, s.MaxBytes)
	}

	if s.SpoolDir != "" {
		if err := os.MkdirAll(s.SpoolDir, 0o700); err != nil {
			return nil, err
		}
	}

	file, err := os.CreateTemp(s.SpoolDir, "bucket-object-*")
	if err != nil {
		return nil, err
	}

	return &spooledObject{
		file: file,
		info: bucket.ObjectInfo{
			Key:                chunk.Key,
			Size:               int64(attributes.Size_), // nolint: gosec
			ContentType:        attributes.ContentType,
			ContentEncoding:    attributes.ContentEncoding,
			ContentDisposition: attributes.ContentDisposition,
			ContentLanguage:    attributes.ContentLanguage,
			CacheControl:       attributes.CacheControl,
			Expires:            attributes.Expires,
			Metadata:           attributes.Metadata,
		},
		whole: sha256.New(),
	}, nil
}

// spooledObject is an object in flight, on this provider's disk until it is
// whole and checked.
type spooledObject struct {
	file    *os.File
	info    bucket.ObjectInfo
	whole   hash.Hash
	written int64
	sum     []byte
}

func (o *spooledObject) write(chunk *vtv1.ObjectChunk) error {
	if chunk.Offset != uint64(o.written) { // nolint: gosec
		return fmt.Errorf("%w: chunk offset %d, want %d", ErrChecksumMismatch, chunk.Offset, o.written)
	}

	sum := sha256.Sum256(chunk.Data)
	if !bytes.Equal(sum[:], chunk.SHA256) {
		return fmt.Errorf("%w: chunk at offset %d", ErrChecksumMismatch, chunk.Offset)
	}

	if o.written+int64(len(chunk.Data)) > o.info.Size {
		return fmt.Errorf("%w: an object passes its declared %d bytes", ErrTooLarge, o.info.Size)
	}

	if _, err := o.file.Write(chunk.Data); err != nil {
		return err
	}

	o.whole.Write(chunk.Data)
	o.written += int64(len(chunk.Data))

	return nil
}

// finish checks the object is whole: every declared byte arrived and they
// hash to the digest the source sent last.
func (o *spooledObject) finish(want []byte) error {
	if o.written != o.info.Size {
		return fmt.Errorf("%w: an object ended at %d of its %d bytes", ErrChecksumMismatch, o.written, o.info.Size)
	}

	sum := o.whole.Sum(nil)
	if !bytes.Equal(sum, want) {
		return fmt.Errorf("%w: a whole object", ErrChecksumMismatch)
	}

	o.sum = sum

	return nil
}

func (o *spooledObject) discard() {
	if o == nil {
		return
	}

	_ = o.file.Close()
	_ = os.Remove(o.file.Name())
}
