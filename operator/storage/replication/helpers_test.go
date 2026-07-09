package replication

import (
	"io"

	vtv1 "pkg.akt.dev/go/volume/v1"
)

// chunkSenderFunc adapts a func to ChunkSender.
type chunkSenderFunc func(data, sum []byte, offset uint64) error

func (f chunkSenderFunc) Send(chunk *vtv1.ExportChunk) error {
	return f(chunk.Data, chunk.SHA256, chunk.Offset)
}

// queueReceiver replays recorded frames as a ChunkReceiver.
type queueReceiver struct {
	frames []chunkFrame
	next   int
}

func (r *queueReceiver) Recv() (*vtv1.ExportChunk, error) {
	if r.next >= len(r.frames) {
		return nil, io.EOF
	}

	frame := r.frames[r.next]
	r.next++

	return &vtv1.ExportChunk{
		Data:   frame.data,
		SHA256: frame.sha256,
		Offset: frame.offset,
	}, nil
}
