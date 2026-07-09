package replication

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"cosmossdk.io/log"

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
		log:       logger.With("cmp", "volume-transfer"),
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

// authorize runs the exporter-side gate shared by Export and Status.
func (s *Server) authorize(ctx context.Context, owner, vid string, dseq uint64, requester string) (VolumeInfo, error) {
	peerAddr, err := s.peerID(ctx)
	if err != nil {
		return VolumeInfo{}, status.Error(codes.Unauthenticated, err.Error())
	}

	req := &vtv1.ExportRequest{Owner: owner, Vid: vid, DSeq: dseq, Requester: requester}

	if err := AuthorizeExport(ctx, s.chain, req, peerAddr, s.self); err != nil {
		if errors.Is(err, ErrNotAuthorized) {
			return VolumeInfo{}, status.Error(codes.PermissionDenied, err.Error())
		}

		return VolumeInfo{}, status.Error(codes.Unavailable, err.Error())
	}

	info, err := s.vols.Resolve(ctx, owner, vid)
	if err != nil {
		return VolumeInfo{}, status.Error(codes.NotFound, fmt.Sprintf("volume %s/%s: %s", owner, vid, err.Error()))
	}

	if info.DSeq != dseq {
		return VolumeInfo{}, status.Error(codes.NotFound, fmt.Sprintf("volume %s/%s is not deployment %d here", owner, vid, dseq))
	}

	if !info.Exportable {
		return VolumeInfo{}, status.Error(codes.FailedPrecondition, fmt.Sprintf("volume %s/%s is not exportable", owner, vid))
	}

	return info, nil
}

// Export streams the volume image at the latest held snapshot: full when
// from_snapshot is empty, an incremental diff otherwise, resuming at
// resume_offset.
func (s *Server) Export(req *vtv1.ExportRequest, stream vtv1.VolumeTransfer_ExportServer) error {
	ctx := stream.Context()

	info, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return err
	}

	snap, err := s.exportSnapshot(ctx, info.Ref)
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

// exportSnapshot picks the snapshot exports serve from: the most recent
// held one, or a fresh one when none exists yet. Exports never read the
// live image.
func (s *Server) exportSnapshot(ctx context.Context, ref Ref) (Snapshot, error) {
	snaps, err := s.driver.Snapshots(ctx, ref)
	if err != nil {
		return Snapshot{}, err
	}

	if len(snaps) > 0 {
		return snaps[len(snaps)-1], nil
	}

	return s.driver.Snapshot(ctx, ref)
}

// Status reports the snapshots held, the whole-image digest at the latest
// one, and the sync lag a destination syncing from it would carry.
func (s *Server) Status(ctx context.Context, req *vtv1.StatusRequest) (*vtv1.StatusResponse, error) {
	info, err := s.authorize(ctx, req.Owner, req.Vid, req.DSeq, req.Requester)
	if err != nil {
		return nil, err
	}

	snap, err := s.exportSnapshot(ctx, info.Ref)
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
	}

	for _, sn := range snaps {
		resp.Snapshots = append(resp.Snapshots, sn.ID)
	}

	if !snap.CreatedAt.IsZero() {
		resp.SyncLagSeconds = int64(s.now().Sub(snap.CreatedAt).Seconds())
	}

	return resp, nil
}
