package provider

import (
	"context"
	"errors"
	"time"

	"github.com/boz/go-lifecycle"

	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkquery "github.com/cosmos/cosmos-sdk/types/query"

	"cosmossdk.io/log"

	aclient "pkg.akt.dev/go/node/client/v1beta3"
	mv1 "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"

	"github.com/akash-network/provider/cluster"
	"github.com/akash-network/provider/session"
)

// volumeReclaimSweepInterval paces the sweep: a reclaimed volume lease
// closes within this long of its deadline.
const volumeReclaimSweepInterval = 30 * time.Second

// volumeReclaimer closes this provider's volume leases whose reclamation
// window ran out. A compute lease's deployment manager arms that close when
// the reclamation starts; a volume lease has no manager, so without this a
// tenant's move would hold the lease, and its escrow, until the tenant
// closed it a second time. Every sweep re-derives what is due from the
// chain, so a restart loses nothing.
type volumeReclaimer struct {
	session  session.Session
	cclient  cluster.Client
	interval time.Duration
	log      log.Logger
	lc       lifecycle.Lifecycle
}

func newVolumeReclaimer(ctx context.Context, clientSession session.Session, cclient cluster.Client) *volumeReclaimer {
	vr := &volumeReclaimer{
		session:  clientSession,
		cclient:  cclient,
		interval: volumeReclaimSweepInterval,
		log:      clientSession.Log().With("module", "volume-reclaimer"),
		lc:       lifecycle.New(),
	}

	go vr.lc.WatchContext(ctx)
	go vr.run(ctx)

	return vr
}

func (vr *volumeReclaimer) Close() error {
	vr.lc.Shutdown(nil)
	return vr.lc.Error()
}

func (vr *volumeReclaimer) run(ctx context.Context) {
	defer vr.lc.ShutdownCompleted()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(vr.interval)
	defer ticker.Stop()

	for {
		select {
		case err := <-vr.lc.ShutdownRequest():
			vr.lc.ShutdownInitiated(err)
			return
		case <-ticker.C:
			if err := vr.sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
				vr.log.Error("closing reclaimed volume leases", "err", err)
			}
		}
	}
}

// sweep closes every reclaiming volume lease whose deadline has passed in
// block time.
func (vr *volumeReclaimer) sweep(ctx context.Context) error {
	leases, err := vr.reclaimingLeases(ctx)
	if err != nil || len(leases) == 0 {
		return err
	}

	sync, err := vr.session.Client().Node().SyncInfo(ctx)
	if err != nil {
		return err
	}

	// a node catching up reports an old block time; the chain would refuse
	// the close and still charge for it
	if sync.CatchingUp {
		return nil
	}

	now := sync.LatestBlockTime.Unix()

	for _, lease := range leases {
		if lease.Reclamation == nil || lease.Reclamation.Deadline > now {
			continue
		}

		// compute leases close through their deployment manager
		if _, err := vr.cclient.LeaseVolume(ctx, lease.ID); err != nil {
			continue
		}

		if err := vr.closeLease(ctx, lease.ID); err != nil {
			vr.log.Error("closing reclaimed volume lease", "lid", lease.ID, "err", err)
			continue
		}

		vr.log.Info("closed reclaimed volume lease", "lid", lease.ID, "reason", lease.Reclamation.Reason.String())
	}

	return nil
}

func (vr *volumeReclaimer) reclaimingLeases(ctx context.Context) ([]mv1.Lease, error) {
	var (
		leases  []mv1.Lease
		nextKey []byte
	)

	for {
		req := &mvbeta.QueryLeasesRequest{
			Filters: mv1.LeaseFilters{
				Provider: vr.session.Provider().Owner,
				State:    mv1.LeaseReclaiming.String(),
			},
		}

		if nextKey != nil {
			req.Pagination = &sdkquery.PageRequest{Key: nextKey}
		}

		resp, err := vr.session.Client().Query().Market().Leases(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, entry := range resp.Leases {
			leases = append(leases, entry.Lease)
		}

		if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
			return leases, nil
		}

		nextKey = resp.Pagination.NextKey
	}
}

// closeLease closes a reclaimed volume lease. The chain records the
// reclamation's own reason (volume_migrate or volume_evict) for it, which
// is what re-lists the volume, whatever provider reason the close carries.
func (vr *volumeReclaimer) closeLease(ctx context.Context, lid mv1.LeaseID) error {
	msg := &mvbeta.MsgCloseBid{
		ID:     lid.BidID(),
		Reason: mv1.LeaseClosedReasonDecommissioned,
	}

	_, err := vr.session.Client().Tx().BroadcastMsgs(ctx, []sdk.Msg{msg}, aclient.WithResultCodeAsError())

	return err
}
