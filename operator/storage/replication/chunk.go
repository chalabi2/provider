package replication

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	vtv1 "pkg.akt.dev/go/volume/v1"
)

// ChunkSender is the send half of the Export stream
// (vtv1.VolumeTransfer_ExportServer satisfies it).
type ChunkSender interface {
	Send(*vtv1.ExportChunk) error
}

// ChunkReceiver is the receive half of the Export stream
// (vtv1.VolumeTransfer_ExportClient satisfies it).
type ChunkReceiver interface {
	Recv() (*vtv1.ExportChunk, error)
}

// SendChunks frames r into ExportChunks of chunkSize bytes, each stamped
// with its stream offset and sha256. resumeOffset bytes are read and
// discarded first so a re-driven export resumes exactly where the previous
// stream broke; offsets on the wire are absolute stream offsets. Returns
// the offset reached (== total stream length on a complete run).
func SendChunks(sender ChunkSender, r io.Reader, resumeOffset uint64, chunkSize int) (uint64, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	if resumeOffset > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(resumeOffset)); err != nil { // nolint: gosec
			return 0, fmt.Errorf("%w: resume offset %d beyond stream: %s", ErrReplication, resumeOffset, err.Error())
		}
	}

	offset := resumeOffset
	buf := make([]byte, chunkSize)

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			sum := sha256.Sum256(buf[:n])

			if serr := sender.Send(&vtv1.ExportChunk{
				Data:   append([]byte(nil), buf[:n]...),
				SHA256: sum[:],
				Offset: offset,
			}); serr != nil {
				return offset, serr
			}

			offset += uint64(n) // nolint: gosec
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
			return offset, nil
		default:
			return offset, err
		}
	}
}

// ReceiveChunks drains the stream into w, verifying each chunk's sha256
// and that offsets are contiguous from offset. Returns the offset reached,
// which is the resume_offset to re-request after a broken stream. A
// checksum or continuity violation aborts with ErrChecksumMismatch; the
// caller must not apply a partial, unverified stream.
func ReceiveChunks(recv ChunkReceiver, w io.WriterAt, offset uint64) (uint64, error) {
	for {
		chunk, err := recv.Recv()
		if err == io.EOF {
			return offset, nil
		}
		if err != nil {
			return offset, err
		}

		if chunk.Offset != offset {
			return offset, fmt.Errorf("%w: chunk offset %d, want %d", ErrChecksumMismatch, chunk.Offset, offset)
		}

		sum := sha256.Sum256(chunk.Data)
		if !bytes.Equal(sum[:], chunk.SHA256) {
			return offset, fmt.Errorf("%w: chunk at offset %d", ErrChecksumMismatch, chunk.Offset)
		}

		if _, err := w.WriteAt(chunk.Data, int64(chunk.Offset)); err != nil { // nolint: gosec
			return offset, err
		}

		offset += uint64(len(chunk.Data))
	}
}
