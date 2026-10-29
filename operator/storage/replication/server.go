package replication

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"cosmossdk.io/log"

	mv1 "pkg.akt.dev/go/node/market/v1"
	vtv1 "pkg.akt.dev/go/volume/v1"

	"github.com/akash-network/provider/bucket"
)

// VolumeInfo is what the transfer server needs to know about a local
// volume: where its bytes are and which deployment it serves.
type VolumeInfo struct {
	Ref Ref
	// DSeq of the volume deployment on this (the exporter's) side.
	DSeq uint64
	// Exportable reports whether the volume is in a phase that may serve
	// exports (anything with a provisioned PV that is not being
	// destroyed).
	Exportable bool
	// Incomplete reports a volume whose data is not whole here: a volume
	// moved here whose copy never finished arriving, or a replica whose
	// copy of its primary is missing or being written. It holds nothing
	// anyone may take.
	Incomplete bool
	// Quiesced reports that no workload has the volume mounted, so nothing
	// can write to it. For a bucket: that no key can.
	Quiesced bool
	// Bucket reports an S3 bucket, whose objects move with ExportObjects
	// rather than as an image.
	Bucket bool
	// SyncedAt is, for a replica still following its primary, when the data
	// it holds was taken there; zero for any other volume.
	SyncedAt time.Time
}

// ReasonImportIncomplete is the ErrorInfo reason of a refusal from a
// provider whose own copy of the volume never finished arriving: it never
// held the data, so a destination looks for it further back.
const ReasonImportIncomplete = "VOLUME_IMPORT_INCOMPLETE"

const errorDomain = "akash.network"

// ReasonNotFinal is the ErrorInfo reason of a refusal to export a bucket
// that can still change: its lease here has not closed, or a key can still
// write to it. A destination asks again later.
const ReasonNotFinal = "VOLUME_NOT_FINAL"

func importIncompleteError(owner, vid string) error {
	return reasonError(fmt.Sprintf("volume %s/%s never finished arriving here", owner, vid), ReasonImportIncomplete)
}

func notFinalError(owner, vid string) error {
	return reasonError(fmt.Sprintf("bucket %s/%s can still change", owner, vid), ReasonNotFinal)
}

func reasonError(msg, reason string) error {
	st := status.New(codes.FailedPrecondition, msg)

	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: errorDomain})
	if err != nil {
		return st.Err()
	}

	return detailed.Err()
}

// IsImportIncomplete reports whether err is a source's refusal of a volume
// it never finished importing.
func IsImportIncomplete(err error) bool {
	return hasReason(err, ReasonImportIncomplete)
}

// IsNotFinal reports whether err is a source's refusal of a bucket that can
// still change.
func IsNotFinal(err error) bool {
	return hasReason(err, ReasonNotFinal)
}

func hasReason(err error, reason string) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == reason && info.Domain == errorDomain {
			return true
		}
	}

	return false
}

// VolumeSource resolves a local volume by its data-continuity identity.
type VolumeSource interface {
	Resolve(ctx context.Context, owner, vid string) (VolumeInfo, error)
}

// ExportRecorder keeps, on a volume's record, that its final copy reached
// the provider it moved to: the volume is held for that provider until
// then, and a restart must not forget it arrived.
type ExportRecorder interface {
	RecordExport(ctx context.Context, owner, vid, to string, at time.Time) error
}

// PeerIDFn extracts the authenticated peer's on-chain address from the
// request context. The default reads the mTLS peer certificate's common
// name - the x/cert identity the TLS layer already validated against
// chain state.
type PeerIDFn func(ctx context.Context) (string, error)

// Server serves VolumeTransfer on the provider gateway
// (IMPLEMENTATION.md §2.7): mTLS with x/cert provider identities both
// directions, authorization derived from chain state on both ends,
// chunked and resumable exports with per-chunk sha256.
type Server struct {
	driver    Driver
	chain     ChainState
	vols      VolumeSource
	self      string
	peerID    PeerIDFn
	chunkSize int
	now       func() time.Time
	log       log.Logger
	// buckets serves the objects of moving buckets; nil serves none
	buckets bucket.Backend
	// exports records the final copies that reached the provider a volume
	// moved to; nil records none
	exports ExportRecorder

	// replicaEvery spaces the snapshots taken for replicas: one asking
	// sooner gets the last one
	replicaEvery time.Duration

	// final holds the snapshot taken once a volume went final, keyed by the
	// volume and the lease whose close made it final; every later export of
	// that close serves it
	mu    sync.Mutex
	final map[string]string
	// replicaTaken records when a snapshot was last taken for a volume's
	// replicas, for a driver that cannot date its snapshots
	replicaTaken map[string]time.Time
}

// defaultReplicaEvery is the least time between two snapshots taken for a
// volume's replicas, whatever pace they sync at.
const defaultReplicaEvery = time.Minute

var _ vtv1.VolumeTransferServer = (*Server)(nil)

// ServerOpt tweaks a transfer server.
type ServerOpt func(*Server)

// WithPeerID overrides peer-identity extraction (tests).
func WithPeerID(fn PeerIDFn) ServerOpt {
	return func(s *Server) { s.peerID = fn }
}

// WithChunkSize overrides the export chunk size.
func WithChunkSize(size int) ServerOpt {
	return func(s *Server) { s.chunkSize = size }
}

// WithBuckets serves the objects of the buckets that move away.
func WithBuckets(buckets bucket.Backend) ServerOpt {
	return func(s *Server) { s.buckets = buckets }
}

// WithExportRecorder records each final copy that reached the provider a
// volume moved to.
func WithExportRecorder(exports ExportRecorder) ServerOpt {
	return func(s *Server) { s.exports = exports }
}

// withClock overrides the server's clock (tests).
func withClock(now func() time.Time) ServerOpt {
	return func(s *Server) { s.now = now }
}

// NewServer builds the VolumeTransfer server. self is this provider's
// address - the exporter identity that never authorizes itself.
func NewServer(driver Driver, chain ChainState, vols VolumeSource, self string, logger log.Logger, opts ...ServerOpt) *Server {
	s := &Server{
		driver:       driver,
		chain:        chain,
		vols:         vols,
		self:         self,
		peerID:       peerFromMTLS,
		chunkSize:    DefaultChunkSize,
		now:          time.Now,
		log:          logger.With("module", "volume-transfer"),
		replicaEvery: defaultReplicaEvery,
		final:        make(map[string]string),
		replicaTaken: make(map[string]time.Time),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// peerFromMTLS reads the peer's on-chain address from the mTLS client
// certificate the TLS handshake already validated against x/cert chain
// state (gateway/utils.NewServerTLSConfig).
func peerFromMTLS(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("%w: no peer on connection", ErrNotAuthorized)
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", fmt.Errorf("%w: connection is not TLS", ErrNotAuthorized)
	}

	certs := tlsInfo.State.PeerCertificates
	if len(certs) != 1 {
		return "", fmt.Errorf("%w: expected exactly one client certificate", ErrNotAuthorized)
	}

	return peerAddress(certs[0])
}

func peerAddress(cert *x509.Certificate) (string, error) {
	if cert.Subject.CommonName == "" {
		return "", fmt.Errorf("%w: client certificate carries no identity", ErrNotAuthorized)
	}

	return cert.Subject.CommonName, nil
}

// exportAuth is what an authorized export or status request may read.
type exportAuth struct {
	info VolumeInfo
	// finalKey is, once the volume is final (this provider's lease on it
	// has closed and no workload has it mounted, so it cannot change
	// again), the key its final snapshot is kept under; empty otherwise
	finalKey string
	// replica reports a requester syncing a replica of the volume
	replica bool
	// maxReplicas is how many replicas the volume's lease here serves
	maxReplicas uint32
}

// authorize runs the exporter-side gate shared by Export and Status.
func (s *Server) authorize(ctx context.Context, owner, vid string, dseq uint64, requester string) (exportAuth, error) {
	peerAddr, err := s.peerID(ctx)
	if err != nil {
		return exportAuth{}, status.Error(codes.Unauthenticated, err.Error())
	}

	req := &vtv1.ExportRequest{Owner: owner, Vid: vid, DSeq: dseq, Requester: requester}

	grant, err := authorizeExport(ctx, s.chain, req, peerAddr, s.self)
	if err != nil {
		if errors.Is(err, ErrNotAuthorized) {
			return exportAuth{}, status.Error(codes.PermissionDenied, err.Error())
		}

		return exportAuth{}, status.Error(codes.Unavailable, err.Error())
	}

	info, err := s.vols.Resolve(ctx, owner, vid)
	if err != nil {
		return exportAuth{}, status.Error(codes.NotFound, fmt.Sprintf("volume %s/%s: %s", owner, vid, err.Error()))
	}

	if info.DSeq != dseq {
		return exportAuth{}, status.Error(codes.NotFound, fmt.Sprintf("volume %s/%s is not deployment %d here", owner, vid, dseq))
	}

	if info.Incomplete {
		return exportAuth{}, importIncompleteError(owner, vid)
	}

	if !info.Exportable {
		return exportAuth{}, status.Error(codes.FailedPrecondition, fmt.Sprintf("volume %s/%s is not exportable", owner, vid))
	}

	auth := exportAuth{info: info, replica: grant.replica}

	own, held := latestLease(grant.leases, vid, dseq, s.self)
	if held {
		auth.maxReplicas = own.Policy.MaxReplicas
	}

	if info.Quiesced && held && own.State == mv1.LeaseClosed {
		auth.finalKey = fmt.Sprintf("%s@%d", info.Ref.Name, own.OSeq)
	}

	return auth, nil
}

// Export streams the volume image at the latest held snapshot: full when
// from_snapshot is empty, an incremental diff otherwise, resuming at
// resume_offset.
func (s *Server) Export(req *vtv1.ExportRequest, stream vtv1.VolumeTransfer_ExportServer) error {
	ctx := stream.Context()

	auth, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return err
	}

	info := auth.info

	if info.Bucket {
		return bucketImageError(req.Owner, req.Vid)
	}

	snap, err := s.exportSnapshot(ctx, info.Ref, auth.finalKey)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if req.FromSnapshot == snap.ID {
		// nothing newer than the requester's base; empty stream
		return nil
	}

	rc, err := s.driver.Export(ctx, info.Ref, snap.ID, req.FromSnapshot)
	if err != nil {
		if errors.Is(err, ErrUnknownSnapshot) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}

		return status.Error(codes.Internal, err.Error())
	}
	defer func() { _ = rc.Close() }()

	s.log.Info("volume export", "volume", req.Owner+"/"+req.Vid, "requester", req.Requester,
		"snapshot", snap.ID, "from", req.FromSnapshot, "resume", req.ResumeOffset)

	if _, err := SendChunks(stream, rc, req.ResumeOffset, s.chunkSize); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	s.recordExport(ctx, auth, req.Owner, req.Vid, req.Requester)

	return nil
}

// recordExport keeps that a final export ran to its end: the provider
// holding the volume's new lease has its last state. A replica's pulls,
// and the rounds before the volume went final, are no such copy.
func (s *Server) recordExport(ctx context.Context, auth exportAuth, owner, vid, requester string) {
	if s.exports == nil || auth.replica || auth.finalKey == "" {
		return
	}

	if err := s.exports.RecordExport(ctx, owner, vid, requester, s.now()); err != nil {
		// the volume is then held to its bound
		s.log.Error("recording the volume's final copy", "volume", auth.info.Ref.Name, "requester", requester, "err", err)
		return
	}

	s.log.Info("volume final copy sent whole", "volume", auth.info.Ref.Name, "requester", requester)
}

// ExportObjects streams every object of a bucket that moves to the
// requester, in key order, from after start_after. It serves a bucket only
// once it is final: its lease here closed and no key can write to it, so
// the requester copies its last state.
func (s *Server) ExportObjects(req *vtv1.ExportObjectsRequest, stream vtv1.VolumeTransfer_ExportObjectsServer) error {
	ctx := stream.Context()

	auth, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return err
	}

	info := auth.info

	if !info.Bucket || s.buckets == nil {
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("volume %s/%s is not a bucket served here", req.Owner, req.Vid))
	}

	if auth.finalKey == "" {
		return notFinalError(req.Owner, req.Vid)
	}

	objects, err := s.buckets.Objects(ctx, bucket.RefFor(info.Ref.Name, req.Owner, req.Vid))
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		if err := objects.Close(); err != nil {
			s.log.Error("closing bucket export", "volume", info.Ref.Name, "err", err)
		}
	}()

	s.log.Info("bucket export", "volume", info.Ref.Name, "requester", req.Requester, "resumed", req.StartAfter != "")

	sent, size, err := SendObjects(ctx, stream, objects, req.StartAfter, s.chunkSize)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	s.log.Info("bucket export done", "volume", info.Ref.Name, "requester", req.Requester, "objects", sent, "bytes", size)

	s.recordExport(ctx, auth, req.Owner, req.Vid, req.Requester)

	return nil
}

func bucketImageError(owner, vid string) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("volume %s/%s is a bucket: its objects move with ExportObjects", owner, vid))
}

// exportSnapshot picks the snapshot exports serve from. While the volume
// can still change that is the most recent one held, or a fresh one when
// none is. Once it is final, it is a snapshot taken after that, so it holds
// the last write. Exports never read the live image.
func (s *Server) exportSnapshot(ctx context.Context, ref Ref, finalKey string) (Snapshot, error) {
	if finalKey != "" {
		return s.finalSnapshot(ctx, ref, finalKey)
	}

	snaps, err := s.driver.Snapshots(ctx, ref)
	if err != nil {
		return Snapshot{}, err
	}

	if len(snaps) > 0 {
		return snaps[len(snaps)-1], nil
	}

	return s.driver.Snapshot(ctx, ref)
}

// replicaSnapshot picks the snapshot a replica syncs to: a fresh one, so its
// copy follows the volume, unless one was taken in the last replicaEvery,
// which a replica asking that soon gets instead. Taking one prunes the
// oldest down to keep, enough that each replica's diff base is likely still
// held at its next round; one whose base went falls back to a full copy.
func (s *Server) replicaSnapshot(ctx context.Context, ref Ref, keep int) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snaps, err := s.driver.Snapshots(ctx, ref)
	if err != nil {
		return Snapshot{}, err
	}

	now := s.now()

	if n := len(snaps); n > 0 {
		latest := snaps[n-1]

		if now.Sub(latest.CreatedAt) < s.replicaEvery || now.Sub(s.replicaTaken[ref.Name]) < s.replicaEvery {
			return latest, nil
		}
	}

	snap, err := s.driver.Snapshot(ctx, ref)
	if err != nil {
		return Snapshot{}, err
	}

	s.replicaTaken[ref.Name] = now

	if err := s.driver.Prune(ctx, ref, keep); err != nil {
		s.log.Error("pruning volume snapshots", "volume", ref.Name, "err", err)
	}

	return snap, nil
}

// replicaSnapshotsKept is how many snapshots a volume with maxReplicas
// replicas keeps: one per replica's diff base, the one being served, and
// one more for a replica mid-transfer when the next is taken.
func replicaSnapshotsKept(maxReplicas uint32) int {
	return int(maxReplicas) + 2
}

// finalSnapshot takes the volume's final snapshot once and serves it from
// then on. A restart takes another, which holds the same data.
func (s *Server) finalSnapshot(ctx context.Context, ref Ref, key string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id, taken := s.final[key]; taken {
		snaps, err := s.driver.Snapshots(ctx, ref)
		if err != nil {
			return Snapshot{}, err
		}

		for _, snap := range snaps {
			if snap.ID == id {
				return snap, nil
			}
		}
	}

	snap, err := s.driver.Snapshot(ctx, ref)
	if err != nil {
		return Snapshot{}, err
	}

	s.final[key] = snap.ID

	s.log.Info("volume final snapshot taken", "volume", ref.Name, "snapshot", snap.ID)

	return snap, nil
}

// Status reports the snapshots held, the whole-image digest at the latest
// one, the sync lag a destination syncing from it would carry, and whether
// that snapshot is final.
//
// A replica asking is handed a fresh snapshot (replicaSnapshot): its next
// export carries the volume as it is now, not as the first replica sync
// found it.
func (s *Server) Status(ctx context.Context, req *vtv1.StatusRequest) (*vtv1.StatusResponse, error) {
	auth, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return nil, err
	}

	info := auth.info

	if info.Bucket {
		return nil, bucketImageError(req.Owner, req.Vid)
	}

	var snap Snapshot

	if auth.replica && auth.finalKey == "" {
		snap, err = s.replicaSnapshot(ctx, info.Ref, replicaSnapshotsKept(auth.maxReplicas))
	} else {
		snap, err = s.exportSnapshot(ctx, info.Ref, auth.finalKey)
	}

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	snaps, err := s.driver.Snapshots(ctx, info.Ref)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	digest, err := s.driver.Digest(ctx, info.Ref, snap.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &vtv1.StatusResponse{
		Snapshots:      make([]string, 0, len(snaps)),
		LatestSnapshot: snap.ID,
		Size_:          digest.Size,
		SHA256:         digest.SHA256,
		Final:          auth.finalKey != "",
	}

	for _, sn := range snaps {
		resp.Snapshots = append(resp.Snapshots, sn.ID)
	}

	if !snap.CreatedAt.IsZero() {
		resp.SyncLagSeconds = int64(s.now().Sub(snap.CreatedAt).Seconds())
	}

	// a replica's data is as old as its last copy of its primary
	if !info.SyncedAt.IsZero() {
		resp.SyncLagSeconds = max(resp.SyncLagSeconds, int64(s.now().Sub(info.SyncedAt).Seconds()))
	}

	return resp, nil
}
