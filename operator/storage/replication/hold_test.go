package replication

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mv1 "pkg.akt.dev/go/node/market/v1"

	"github.com/akash-network/provider/bucket"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

// exportRecord is one final copy the server recorded as sent whole.
type exportRecord struct {
	owner, vid, to string
}

// fakeExports records what the transfer server reports as sent whole.
type fakeExports struct {
	mu      sync.Mutex
	records []exportRecord
	err     error
}

func (f *fakeExports) RecordExport(_ context.Context, owner, vid, to string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	f.records = append(f.records, exportRecord{owner: owner, vid: vid, to: to})

	return nil
}

func (f *fakeExports) recorded() []exportRecord {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]exportRecord(nil), f.records...)
}

func TestTransferRecordsAFinalCopySentWhole(t *testing.T) {
	ctx := context.Background()
	exports := &fakeExports{}

	w := transferScaffold(t, WithExportRecorder(exports))
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})

	dst := NewFileDriver(t.TempDir())
	syncer := w.syncer(t, dst, Ref{Name: w.ref.Name, Path: t.TempDir()})

	// the source's lease still runs: its volume can change, nothing is final
	_, err := syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, exports.recorded())

	writeTree(t, w.src, w.ref, map[string]string{"app.db": "the last write"})
	w.goFinal()

	_, err = syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.True(t, syncer.Final())
	require.Equal(t, []exportRecord{{owner: authzOwner, vid: authzVID, to: authzRequester}}, exports.recorded())

	// in sync with the final snapshot: nothing more is sent, or recorded
	_, err = syncer.SyncOnce(ctx)
	require.NoError(t, err)
	require.Len(t, exports.recorded(), 1)
}

// breakingDriver serves exports that break after limit bytes.
type breakingDriver struct {
	Driver
	limit int64
}

func (d *breakingDriver) Export(ctx context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error) {
	rc, err := d.Driver.Export(ctx, ref, snapshot, fromSnapshot)
	if err != nil {
		return nil, err
	}

	return &breakingReader{ReadCloser: rc, left: d.limit}, nil
}

type breakingReader struct {
	io.ReadCloser
	left int64
}

func (r *breakingReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, errors.New("disk read error")
	}

	if int64(len(p)) > r.left {
		p = p[:r.left]
	}

	n, err := r.ReadCloser.Read(p)
	r.left -= int64(n)

	return n, err
}

func TestTransferRecordsNoCopyCutShort(t *testing.T) {
	exports := &fakeExports{}

	w := transferScaffold(t, WithExportRecorder(exports))
	w.client.srv.driver = &breakingDriver{Driver: w.src, limit: 64}
	writeTree(t, w.src, w.ref, map[string]string{"app.db": string(make([]byte, 4096))})
	w.goFinal()

	_, err := w.syncer(t, NewFileDriver(t.TempDir()), Ref{Name: w.ref.Name, Path: t.TempDir()}).SyncOnce(context.Background())
	require.Error(t, err)
	require.Empty(t, exports.recorded())
}

func TestTransferRecordsNoReplicaPull(t *testing.T) {
	exports := &fakeExports{}

	w := replicaScaffold(t, 1)
	w.client.srv.exports = exports
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})

	// the primary's lease closed and nothing mounts it: final, yet a
	// replica's pull is no move's copy
	w.chain.update(func(chain *fakeChainState) { chain.leases[0].State = mv1.LeaseClosed })
	w.vols.update(func(info *VolumeInfo) { info.Quiesced = true })

	_, err := w.syncer(t, NewFileDriver(t.TempDir()), Ref{Name: w.ref.Name, Path: t.TempDir()}).SyncOnce(context.Background())
	require.NoError(t, err)
	require.Empty(t, exports.recorded())
}

func TestTransferSendsTheCopyWhenItCannotRecordIt(t *testing.T) {
	exports := &fakeExports{err: errors.New("apiserver unavailable")}

	w := transferScaffold(t, WithExportRecorder(exports))
	writeTree(t, w.src, w.ref, map[string]string{"app.db": "rows"})
	w.goFinal()

	dst := NewFileDriver(t.TempDir())
	dstRef := Ref{Name: w.ref.Name, Path: t.TempDir()}

	// the source then holds the volume to its bound
	_, err := w.syncer(t, dst, dstRef).SyncOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"app.db": "rows"}, readTree(t, dst, dstRef))
}

func TestBucketExportRecordsTheCopySentWhole(t *testing.T) {
	exports := &fakeExports{}

	w := bucketScaffoldNamed(t, bucketVolumeName, WithExportRecorder(exports))
	putTestObject(t, w.src, w.ref, bucket.ObjectInfo{Key: "a.txt"}, []byte("objects"))
	w.goFinal()

	complete, err := w.syncer(t, w.client).SyncOnce(context.Background())
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, []exportRecord{{owner: authzOwner, vid: authzVID, to: authzRequester}}, exports.recorded())
}

func TestSourceRecordsAFinalCopyOnTheVolume(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

	src := sourceWorld(t, sourceVolume(crd.VolumePhaseExporting, ""))

	require.NoError(t, src.RecordExport(ctx, authzOwner, authzVID, authzRequester, at))

	vol, err := src.ac.AkashV2beta2().Volumes(sourceNS).Get(ctx, crd.VolumeName(authzOwner, authzVID), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, authzRequester, vol.Status.ExportedTo)
	require.True(t, at.Equal(vol.Status.ExportedAt.Time))
	require.Equal(t, crd.VolumePhaseExporting, vol.Status.Phase)

	// another owner's record under the same name is left alone
	other := sourceVolume(crd.VolumePhaseExporting, "")
	other.Spec.Owner = authzStranger

	require.ErrorIs(t, sourceWorld(t, other).RecordExport(ctx, authzOwner, authzVID, authzRequester, at), ErrReplication)
}
