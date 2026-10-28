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
	// Incomplete reports a volume moved here whose copy never finished
	// arriving: it holds nothing anyone may take.
	Incomplete bool
	// Quiesced reports that no workload has the volume mounted, so nothing
	// can write to it.
	Quiesced bool
}

// ReasonImportIncomplete is the ErrorInfo reason of a refusal from a
// provider whose own copy of the volume never finished arriving: it never
// held the data, so a destination looks for it further back.
const ReasonImportIncomplete = "VOLUME_IMPORT_INCOMPLETE"

const errorDomain = "akash.network"

func importIncompleteError(owner, vid string) error {
	st := status.New(codes.FailedPrecondition, fmt.Sprintf("volume %s/%s never finished arriving here", owner, vid))

	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: ReasonImportIncomplete, Domain: errorDomain})
	if err != nil {
		return st.Err()
	}

	return detailed.Err()
}

// IsImportIncomplete reports whether err is a source's refusal of a volume
// it never finished importing.
func IsImportIncomplete(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == ReasonImportIncomplete && info.Domain == errorDomain {
			return true
		}
	}

	return false
}

// VolumeSource resolves a local volume by its data-continuity identity.
type VolumeSource interface {
	Resolve(ctx context.Context, owner, vid string) (VolumeInfo, error)
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

	// final holds the snapshot taken once a volume went final, keyed by the
	// volume and the lease whose close made it final; every later export of
	// that close serves it
	mu    sync.Mutex
	final map[string]string
}

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

// NewServer builds the VolumeTransfer server. self is this provider's
// address - the exporter identity that never authorizes itself.
func NewServer(driver Driver, chain ChainState, vols VolumeSource, self string, logger log.Logger, opts ...ServerOpt) *Server {
	s := &Server{
		driver:    driver,
		chain:     chain,
		vols:      vols,
		self:      self,
		peerID:    peerFromMTLS,
		chunkSize: DefaultChunkSize,
		now:       time.Now,
		log:       logger.With("module", "volume-transfer"),
		final:     make(map[string]string),
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

// authorize runs the exporter-side gate shared by Export and Status. When
// the volume is final (this provider's lease on it has closed and no
// workload has it mounted, so it cannot change again) it also returns the
// key its final snapshot is kept under; empty otherwise.
func (s *Server) authorize(ctx context.Context, owner, vid string, dseq uint64, requester string) (VolumeInfo, string, error) {
	peerAddr, err := s.peerID(ctx)
	if err != nil {
		return VolumeInfo{}, "", status.Error(codes.Unauthenticated, err.Error())
	}

	req := &vtv1.ExportRequest{Owner: owner, Vid: vid, DSeq: dseq, Requester: requester}

	leases, err := authorizeExport(ctx, s.chain, req, peerAddr, s.self)
	if err != nil {
		if errors.Is(err, ErrNotAuthorized) {
			return VolumeInfo{}, "", status.Error(codes.PermissionDenied, err.Error())
		}

		return VolumeInfo{}, "", status.Error(codes.Unavailable, err.Error())
	}

	info, err := s.vols.Resolve(ctx, owner, vid)
	if err != nil {
		return VolumeInfo{}, "", status.Error(codes.NotFound, fmt.Sprintf("volume %s/%s: %s", owner, vid, err.Error()))
	}

	if info.DSeq != dseq {
		return VolumeInfo{}, "", status.Error(codes.NotFound, fmt.Sprintf("volume %s/%s is not deployment %d here", owner, vid, dseq))
	}

	if info.Incomplete {
		return VolumeInfo{}, "", importIncompleteError(owner, vid)
	}

	if !info.Exportable {
		return VolumeInfo{}, "", status.Error(codes.FailedPrecondition, fmt.Sprintf("volume %s/%s is not exportable", owner, vid))
	}

	own, held := latestLease(leases, vid, dseq, s.self)
	if !info.Quiesced || !held || own.State != mv1.LeaseClosed {
		return info, "", nil
	}

	return info, fmt.Sprintf("%s@%d", info.Ref.Name, own.OSeq), nil
}

// Export streams the volume image at the latest held snapshot: full when
// from_snapshot is empty, an incremental diff otherwise, resuming at
// resume_offset.
func (s *Server) Export(req *vtv1.ExportRequest, stream vtv1.VolumeTransfer_ExportServer) error {
	ctx := stream.Context()

	info, finalKey, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return err
	}

	snap, err := s.exportSnapshot(ctx, info.Ref, finalKey)
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

	return nil
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
func (s *Server) Status(ctx context.Context, req *vtv1.StatusRequest) (*vtv1.StatusResponse, error) {
	info, finalKey, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return nil, err
	}

	snap, err := s.exportSnapshot(ctx, info.Ref, finalKey)
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
		Final:          finalKey != "",
	}

	for _, sn := range snaps {
		resp.Snapshots = append(resp.Snapshots, sn.ID)
	}

	if !snap.CreatedAt.IsZero() {
		resp.SyncLagSeconds = int64(s.now().Sub(snap.CreatedAt).Seconds())
	}

	return resp, nil
}
