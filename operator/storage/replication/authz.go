package replication

import (
	"context"
	"errors"
	"fmt"

	sdkquery "github.com/cosmos/cosmos-sdk/types/query"

	aclient "pkg.akt.dev/go/node/client"
	dv1 "pkg.akt.dev/go/node/deployment/v1"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta5"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v2beta1"
	vtv1 "pkg.akt.dev/go/volume/v1"
)

var (
	// ErrNotAuthorized flags a transfer request chain state does not
	// back. No trust in self-asserted identity anywhere: the mTLS peer
	// must match the request, and the chain must record the relationship.
	ErrNotAuthorized = errors.New("volume replication: not authorized")
)

// VolumeLease is the chain-derived view transfer authorization decides
// from: one lease on a volume group related to the (owner, vid) under
// transfer, with the group's volume policy.
type VolumeLease struct {
	// Provider holds the lease.
	Provider string
	// DSeq of the volume deployment the lease serves.
	DSeq uint64
	// State of the lease.
	State mv1.Lease_State
	// Policy is the group's VolumePolicy.
	Policy dv1.VolumePolicy
}

// ChainState is the narrow chain read both authorization ends need:
// every lease whose volume group names (owner, vid) directly (its own
// vid) or as a replication target (replica_of). Answers are re-derived
// from the chain on every call - nothing is trusted from the peer.
type ChainState interface {
	VolumeLeases(ctx context.Context, owner, vid string) ([]VolumeLease, error)
}

// AuthorizeExport is the exporter-side gate (DESIGN.md §9.1). The
// requester's cert-bound address (peer) must match the request, and chain
// state must record it as either:
//
//   - migration: the provider of a live lease on this volume's own group
//     (same owner/vid/dseq) - the winner of the re-ordered volume order;
//     the exporter itself (self) never qualifies, or
//   - replication: the provider of an active lease on a volume whose
//     replica_of names this volume (same owner).
func AuthorizeExport(ctx context.Context, cs ChainState, req *vtv1.ExportRequest, peer, self string) error {
	if req.Requester == "" || peer != req.Requester {
		return fmt.Errorf("%w: requester %q does not match peer identity %q", ErrNotAuthorized, req.Requester, peer)
	}

	if req.Requester == self {
		return fmt.Errorf("%w: requester is the exporter itself", ErrNotAuthorized)
	}

	leases, err := cs.VolumeLeases(ctx, req.Owner, req.Vid)
	if err != nil {
		return err
	}

	for _, lease := range leases {
		if lease.Provider != req.Requester {
			continue
		}

		if lease.State != mv1.LeaseActive {
			continue
		}

		// migration: the re-ordered lease lives on the same deployment
		if lease.Policy.Vid == req.Vid && lease.DSeq == req.DSeq {
			return nil
		}

		// replication: the requester's volume names this one as its target
		if ref := lease.Policy.ReplicaOf; ref != nil && ref.Owner == req.Owner && ref.Name == req.Vid {
			return nil
		}
	}

	return fmt.Errorf("%w: no chain record entitles %q to volume %s/%s", ErrNotAuthorized, req.Requester, req.Owner, req.Vid)
}

// AuthorizeImport is the importer-side gate: before applying any byte the
// destination verifies the source (the provider it dialed, cert-bound) is,
// per chain state, the provider of the volume's own lease - the
// closed/reclaiming lease for a migration, the active lease of the
// replica_of target for a replica.
func AuthorizeImport(ctx context.Context, cs ChainState, owner, vid, source string, replica bool) error {
	leases, err := cs.VolumeLeases(ctx, owner, vid)
	if err != nil {
		return err
	}

	for _, lease := range leases {
		if lease.Provider != source || lease.Policy.Vid != vid {
			continue
		}

		if replica {
			if lease.State == mv1.LeaseActive {
				return nil
			}

			continue
		}

		// migration: reclaiming (window running) or already closed
		// (cascade-detach done); an active lease also qualifies during
		// the pre-close overlap
		switch lease.State {
		case mv1.LeaseActive, mv1.LeaseReclaiming, mv1.LeaseClosed, mv1.LeaseInsufficientFunds:
			return nil
		}
	}

	return fmt.Errorf("%w: no chain record backs %q as source of volume %s/%s", ErrNotAuthorized, source, owner, vid)
}

// queryChainState derives VolumeLeases from the chain query client: the
// owner's leases joined with their groups' volume policies.
type queryChainState struct {
	qc aclient.QueryClient
}

// NewChainState wraps an akash query client as the authorization
// ChainState.
func NewChainState(qc aclient.QueryClient) ChainState {
	return &queryChainState{qc: qc}
}

func (c *queryChainState) VolumeLeases(ctx context.Context, owner, vid string) ([]VolumeLease, error) {
	var out []VolumeLease

	groups := make(map[string]*dv1.VolumePolicy)

	var nextKey []byte

	for {
		req := &mvbeta.QueryLeasesRequest{
			Filters: mv1.LeaseFilters{Owner: owner},
		}

		if nextKey != nil {
			req.Pagination = &sdkquery.PageRequest{Key: nextKey}
		}

		resp, err := c.qc.Market().Leases(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, entry := range resp.Leases {
			lid := entry.Lease.ID

			gkey := fmt.Sprintf("%d/%d", lid.DSeq, lid.GSeq)

			policy, hit := groups[gkey]
			if !hit {
				gresp, err := c.qc.Deployment().Group(ctx, &dtypes.QueryGroupRequest{
					ID: dv1.GroupID{Owner: lid.Owner, DSeq: lid.DSeq, GSeq: lid.GSeq},
				})
				if err != nil {
					return nil, err
				}

				policy = gresp.Group.GroupSpec.Volume
				groups[gkey] = policy
			}

			if policy == nil {
				continue
			}

			related := policy.Vid == vid ||
				(policy.ReplicaOf != nil && policy.ReplicaOf.Owner == owner && policy.ReplicaOf.Name == vid)

			if !related {
				continue
			}

			out = append(out, VolumeLease{
				Provider: lid.Provider,
				DSeq:     lid.DSeq,
				State:    entry.Lease.State,
				Policy:   *policy,
			})
		}

		if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
			break
		}

		nextKey = resp.Pagination.NextKey
	}

	return out, nil
}
