package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	vtv1 "pkg.akt.dev/go/volume/v1"
)

// defaultPullAttempts bounds resume retries within a single pull; each
// retry re-requests from the verified offset, so progress is monotonic.
const defaultPullAttempts = 5

// Pull drives one resumable export: it streams chunks into spool
// (verifying per-chunk sha256 and offset continuity) and, on a broken
// stream, re-requests from the last verified offset up to attempts times.
// Returns the total bytes landed. A checksum violation aborts immediately:
// resuming past corrupt data is never attempted.
func Pull(ctx context.Context, client vtv1.VolumeTransferClient, req *vtv1.ExportRequest, spool *os.File, attempts int) (uint64, error) {
	if attempts < 1 {
		attempts = defaultPullAttempts
	}

	offset := req.ResumeOffset

	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		r := *req
		r.ResumeOffset = offset

		stream, err := client.Export(ctx, &r)
		if err != nil {
			lastErr = err
			continue
		}

		reached, err := ReceiveChunks(stream, spool, offset)
		if err == nil {
			return reached, nil
		}

		if errors.Is(err, ErrChecksumMismatch) {
			return reached, err
		}

		if ctx.Err() != nil {
			return reached, ctx.Err()
		}

		// broken stream: resume from the verified offset
		offset = reached
		lastErr = err
	}

	return offset, fmt.Errorf("%w: pull exhausted %d attempts: %s", ErrReplication, attempts, lastErr.Error())
}

// Syncer pulls a volume from a source provider over VolumeTransfer and
// applies it through the local driver: an initial full export, then
// incremental diffs from the last applied snapshot. It backs both the
// migration destination and the replica_of periodic sync.
type Syncer struct {
	// Client is the dialed source provider (mTLS with x/cert identities).
	Client vtv1.VolumeTransferClient
	// Driver applies the pulled streams locally.
	Driver Driver
	// Ref locates the local destination image.
	Ref Ref
	// Owner, Vid, DSeq identify the volume on the EXPORTER's side.
	Owner string
	Vid   string
	DSeq  uint64
	// Requester is this (the destination) provider's address; it must
	// match our client certificate.
	Requester string
	// SpoolDir holds in-flight pull spools; empty uses the OS temp dir.
	SpoolDir string

	// last is the last snapshot applied locally - the next diff base.
	last string
}

// Last returns the last applied snapshot id ("" before the first sync).
func (s *Syncer) Last() string {
	return s.last
}

// SyncOnce performs one sync round: query Status, pull what is newer than
// the last applied snapshot (full on the first round or when the source no
// longer holds our base), verify the whole-image digest, apply. Returns
// the snapshot applied, or "" when already in sync.
func (s *Syncer) SyncOnce(ctx context.Context) (string, error) {
	st, err := s.Client.Status(ctx, &vtv1.StatusRequest{
		Owner:     s.Owner,
		Vid:       s.Vid,
		DSeq:      s.DSeq,
		Requester: s.Requester,
	})
	if err != nil {
		return "", err
	}

	if st.LatestSnapshot == "" || st.LatestSnapshot == s.last {
		return "", nil
	}

	from := ""
	if s.last != "" {
		// diff only if the source still holds our base
		for _, held := range st.Snapshots {
			if held == s.last {
				from = s.last
				break
			}
		}
	}

	spool, err := os.CreateTemp(s.SpoolDir, "volume-pull-*")
	if err != nil {
		return "", err
	}

	defer func() {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
	}()

	if _, err := Pull(ctx, s.Client, &vtv1.ExportRequest{
		Owner:        s.Owner,
		Vid:          s.Vid,
		DSeq:         s.DSeq,
		Requester:    s.Requester,
		FromSnapshot: from,
	}, spool, 0); err != nil {
		return "", err
	}

	if from == "" {
		// a full image is verifiable against the source's whole-image
		// digest before it is ever applied
		if err := verifySpool(spool, st.SHA256); err != nil {
			return "", err
		}
	}

	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	if err := s.Driver.Import(ctx, s.Ref, st.LatestSnapshot, from, spool); err != nil {
		return "", err
	}

	if from != "" {
		// a diff is verified post-apply: the reconstructed image must
		// carry the source's whole-image digest
		digest, err := s.Driver.Digest(ctx, s.Ref, st.LatestSnapshot)
		if err != nil {
			return "", err
		}

		if !bytes.Equal(digest.SHA256, st.SHA256) {
			return "", fmt.Errorf("%w: image after diff apply, snapshot %s", ErrChecksumMismatch, st.LatestSnapshot)
		}
	}

	s.last = st.LatestSnapshot

	return st.LatestSnapshot, nil
}

func verifySpool(spool *os.File, want []byte) error {
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return err
	}

	h := sha256.New()
	if _, err := io.Copy(h, spool); err != nil {
		return err
	}

	if !bytes.Equal(h.Sum(nil), want) {
		return fmt.Errorf("%w: whole image %s", ErrChecksumMismatch, filepath.Base(spool.Name()))
	}

	return nil
}
