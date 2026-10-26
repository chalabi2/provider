package bidengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"cosmossdk.io/log"

	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkquery "github.com/cosmos/cosmos-sdk/types/query"

	aclient "pkg.akt.dev/go/node/client/v1beta3"
	dtypes "pkg.akt.dev/go/node/deployment/v1beta4"
	mtypes "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v1beta5"
	metricsutils "pkg.akt.dev/go/util/metrics"

	"github.com/akash-network/provider/cluster"
	ctypes "github.com/akash-network/provider/cluster/types/v1beta3"
	"github.com/akash-network/provider/event"
	"github.com/akash-network/provider/session"
)

// resizeApplyAttempts bounds the group reads of an applied resize. If they all
// fail the reservation keeps the old size, and the inventory under-counts the
// tenant's resized pods until the provider restarts.
const resizeApplyAttempts = 5

var leaseResizeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "provider_lease_resize",
	Help: "Lease resize requests answered, by answer",
}, []string{"result"})

// errResizesNextKeyRepeated is returned when a page of pending lease resizes
// names the key it was requested with as the key of the following page.
var errResizesNextKeyRepeated = errors.New("lease resizes pagination: next key repeats the requested key")

// leaseResize evaluates one pending lease resize request and answers it on
// chain. It is kept apart from order on purpose: an order monitor's exit
// path unreserves by order ID, which for a running lease would drop the
// reservation its workload runs on. Nothing is reserved here; the
// reservation is only swapped once the chain applies an accepted resize.
type leaseResize struct {
	lid mtypes.LeaseID
	// digest identifies the request. The chain stamps it on every request
	// and the answer names it, so an answer never lands on a request that
	// replaced this one, even one made in the same block.
	digest []byte
	// expiresAt is when the chain stops accepting the request.
	expiresAt int64
	// pending is the request when it is already known, as on the startup
	// catch-up; otherwise evaluate looks it up.
	pending *mvbeta.LeaseResize

	cfg     Config
	session session.Session
	cluster cluster.Cluster
	pass    ProviderAttrSignatureService
	storage StorageInventory
	log     log.Logger
	now     func() time.Time

	cancel context.CancelFunc
}

func newLeaseResize(svc *service, lid mtypes.LeaseID, digest []byte, expiresAt int64, pending *mvbeta.LeaseResize) *leaseResize {
	session := svc.session.ForModule("bidengine-resize")

	return &leaseResize{
		lid:       lid,
		digest:    digest,
		expiresAt: expiresAt,
		pending:   pending,
		cfg:       svc.cfg,
		session:   session,
		cluster:   svc.cluster,
		pass:      svc.pass,
		storage:   svc.storageInv,
		log:       session.Log().With("lid", lid),
		now:       time.Now,
	}
}

// gates are the bid gates for the resized group. The local Volume CRD
// pre-checks stay off: they decide whether a volume can be attached or
// adopted, and a resized lease already has its volume.
func (r *leaseResize) gates() bidGates {
	return bidGates{
		cfg:     r.cfg,
		session: r.session,
		pass:    r.pass,
		storage: r.storage,
		log:     r.log,
	}
}

func (r *leaseResize) run(ctx context.Context) {
	msg, err := r.evaluate(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Error("evaluating lease resize", "err", err)
			leaseResizeCounter.WithLabelValues(metricsutils.FailLabel).Inc()
		}

		return
	}

	// nothing to answer, or the chain resolved the request in the meantime
	if msg == nil || ctx.Err() != nil {
		return
	}

	if _, err := r.session.Client().Tx().BroadcastMsgs(ctx, []sdk.Msg{msg}, aclient.WithResultCodeAsError()); err != nil {
		r.log.Error("answering lease resize", "err", err)
		leaseResizeCounter.WithLabelValues(metricsutils.FailLabel).Inc()

		return
	}

	switch msg := msg.(type) {
	case *mvbeta.MsgAcceptLeaseResize:
		r.log.Info("lease resize accepted", "price", msg.Price.String())
		leaseResizeCounter.WithLabelValues("accept").Inc()
	case *mvbeta.MsgRejectLeaseResize:
		r.log.Info("lease resize rejected", "reason", msg.Reason.String())
		leaseResizeCounter.WithLabelValues(msg.Reason.String()).Inc()
	}
}

// evaluate decides the provider's answer to the lease's pending resize: an
// accept at the price the provider would bid on the resized group, or a
// reject saying why not. It returns no message when there is nothing left to
// answer, and an error when it could not decide.
func (r *leaseResize) evaluate(ctx context.Context) (sdk.Msg, error) {
	req := r.pending
	if req == nil {
		var err error
		if req, err = r.pendingRequest(ctx); err != nil {
			return nil, fmt.Errorf("querying pending resize: %w", err)
		}
	}

	switch {
	case req == nil:
		r.log.Info("lease resize no longer pending")
		return nil, nil
	case !bytes.Equal(req.Digest, r.digest):
		// the request that replaced it is evaluated on its own
		r.log.Debug("lease resize replaced", "expires_at", req.ExpiresAt)
		return nil, nil
	case req.Expired(r.now()):
		r.log.Info("lease resize expired", "expires_at", req.ExpiresAt)
		return nil, nil
	}

	res, err := r.session.Client().Query().Deployment().Group(ctx, &dtypes.QueryGroupRequest{ID: r.lid.GroupID()})
	if err != nil {
		return nil, fmt.Errorf("querying group: %w", err)
	}

	group := res.GetGroup()

	next, err := group.GroupSpec.Resize(req.Resources)
	if err != nil {
		r.log.Info("declining lease resize: change not supported", "err", err)
		return r.reject(mtypes.LeaseResizeRejectUnsupported), nil
	}

	resized := group
	resized.GroupSpec = next

	if msg, err := r.checkGates(ctx, &group, &resized); err != nil || msg != nil {
		return msg, err
	}

	allocated, err := r.cluster.CheckResize(r.lid.OrderID(), &resized)
	switch {
	case errors.Is(err, cluster.ErrReservationNotFound):
		r.log.Error("declining lease resize: no reservation held for the lease")
		return r.reject(mtypes.LeaseResizeRejectOther), nil
	case errors.Is(err, ctypes.ErrInsufficientCapacity), errors.Is(err, ctypes.ErrGroupResourceMismatch):
		r.log.Info("declining lease resize: insufficient capacity", "err", err)
		return r.reject(mtypes.LeaseResizeRejectCapacity), nil
	case err != nil:
		return nil, fmt.Errorf("checking capacity: %w", err)
	}

	// priced on the same basis as a bid on the resized group
	price, err := r.cfg.PricingStrategy.CalculatePrice(ctx, Request{
		Owner:              r.lid.Owner,
		GSpec:              &next,
		PricePrecision:     DefaultPricePrecision,
		AllocatedResources: allocated,
	})
	if err != nil {
		return nil, fmt.Errorf("calculating price: %w", err)
	}

	if price.Denom != req.MaxPrice.Denom {
		r.log.Error("declining lease resize: unsupported denomination", "price", price.String(), "max_price", req.MaxPrice.String())
		return r.reject(mtypes.LeaseResizeRejectOther), nil
	}

	if req.MaxPrice.IsLT(price) {
		r.log.Info("declining lease resize: price too high", "price", price.String(), "max_price", req.MaxPrice.String())
		return r.reject(mtypes.LeaseResizeRejectPrice), nil
	}

	return mvbeta.NewMsgAcceptLeaseResize(r.lid, price, r.digest), nil
}

// checkGates runs the bid gates on the resized group and returns the reject
// for a group that fails them. A volume's headroom is checked for its growth
// only: the provider already holds the old size.
func (r *leaseResize) checkGates(ctx context.Context, group, resized *dtypes.Group) (sdk.Msg, error) {
	gates := r.gates()

	if resized.GroupSpec.Volume == nil {
		ok, err := gates.shouldBid(ctx, resized)
		if err != nil {
			return nil, err
		}

		if !ok {
			r.log.Info("declining lease resize: resized group fails the bid gates")
			return r.reject(mtypes.LeaseResizeRejectUnsupported), nil
		}

		return nil, nil
	}

	ok, err := gates.volumeTerms(resized)
	if err != nil {
		return nil, err
	}

	if !ok {
		r.log.Info("declining lease resize: resized volume fails the bid gates")
		return r.reject(mtypes.LeaseResizeRejectUnsupported), nil
	}

	class, size, _ := volumeStorage(&resized.GroupSpec)
	_, held, _ := volumeStorage(&group.GroupSpec)

	var growth uint64
	if size > held {
		growth = size - held
	}

	if !gates.volumeHeadroom(class, growth) {
		r.log.Info("declining lease resize: insufficient storage class headroom", "growth", growth)
		return r.reject(mtypes.LeaseResizeRejectCapacity), nil
	}

	return nil, nil
}

func (r *leaseResize) reject(reason mtypes.LeaseResizeCancelReason) sdk.Msg {
	return mvbeta.NewMsgRejectLeaseResize(r.lid, reason, r.digest)
}

// pendingRequest returns the chain's pending resize of the lease, or nil
// when it has none.
func (r *leaseResize) pendingRequest(ctx context.Context) (*mvbeta.LeaseResize, error) {
	var found *mvbeta.LeaseResize

	req := mvbeta.QueryLeaseResizesRequest{
		Owner:    r.lid.Owner,
		Provider: r.lid.Provider,
	}

	err := walkLeaseResizes(ctx, r.session.Client().Query().Market(), req, func(page []mvbeta.LeaseResize) bool {
		for i := range page {
			if page[i].LeaseID.Equals(r.lid) {
				found = &page[i]
				return false
			}
		}

		return true
	})

	return found, err
}

// walkLeaseResizes pages through the pending lease resizes matching req
// until visit returns false or the last page has been visited.
func walkLeaseResizes(ctx context.Context, qc mvbeta.QueryClient, req mvbeta.QueryLeaseResizesRequest, visit func([]mvbeta.LeaseResize) bool) error {
	var key []byte

	for {
		req.Pagination = &sdkquery.PageRequest{Key: key}

		resp, err := qc.LeaseResizes(ctx, &req)
		if err != nil {
			return err
		}

		if !visit(resp.LeaseResizes) {
			return nil
		}

		var next []byte
		if resp.Pagination != nil {
			next = resp.Pagination.NextKey
		}

		if len(next) == 0 {
			return nil
		}

		// a page naming the requested key as the next one would be fetched forever
		if bytes.Equal(next, key) {
			return fmt.Errorf("%w: %x", errResizesNextKeyRepeated, next)
		}

		key = next
	}
}

// resizesFetcher lists the pending lease resizes addressed to this provider
// once at startup and queues them for run. Requests made while it runs
// arrive as EventLeaseResizeRequested.
func (s *service) resizesFetcher(ctx context.Context, aqc aclient.QueryClient) error {
	req := mvbeta.QueryLeaseResizesRequest{
		Provider: s.session.Provider().Owner,
	}

	for {
		err := walkLeaseResizes(ctx, aqc.Market(), req, func(page []mvbeta.LeaseResize) bool {
			select {
			case s.resizesch <- page:
				return true
			case <-ctx.Done():
				return false
			}
		})

		// check the context rather than the error: gRPC reports cancellation as
		// a status error which does not match context.Canceled
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}

		s.session.Log().Error("querying pending lease resizes", "err", err)

		if errors.Is(err, errResizesNextKeyRepeated) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ordersFetchRetryPeriod):
		}
	}
}

// processResizesPage starts evaluations for a page of the pending lease
// resizes found at startup. Expired requests can no longer be accepted.
func (s *service) processResizesPage(ctx context.Context, page []mvbeta.LeaseResize) {
	now := time.Now()

	for i := range page {
		req := page[i]

		if req.LeaseID.Provider != s.session.Provider().Address().String() || req.Expired(now) {
			continue
		}

		s.session.Log().Info("catching up lease resize", "lid", req.LeaseID)
		s.startResize(ctx, req.LeaseID, req.Digest, req.ExpiresAt, &req)
	}
}

// startResize starts evaluating a lease resize request unless this run has
// seen it already, so a request found by both the catch-up and its event is
// answered once. A request that replaces an earlier one for the same lease
// supersedes it.
func (s *service) startResize(ctx context.Context, lid mtypes.LeaseID, digest []byte, expiresAt int64, pending *mvbeta.LeaseResize) {
	if r := s.resizes[lid]; r != nil {
		if bytes.Equal(r.digest, digest) {
			s.session.Log().Debug("lease resize already seen", "lid", lid)
			return
		}

		r.cancel()
	}

	s.pruneResizes()

	r := newLeaseResize(s, lid, digest, expiresAt, pending)

	rctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	s.resizes[lid] = r
	s.resizeWG.Add(1)

	go func() {
		defer s.resizeWG.Done()
		defer cancel()

		r.run(rctx)
	}()
}

// dropResize forgets the lease's resize request once the chain has resolved
// it, and stops an evaluation still running so it does not answer it. An
// event about another request of the lease leaves the tracked one alone.
func (s *service) dropResize(lid mtypes.LeaseID, digest []byte) {
	if r := s.resizes[lid]; r != nil && bytes.Equal(r.digest, digest) {
		r.cancel()
		delete(s.resizes, lid)
	}
}

// pruneResizes forgets the requests past their expiry, which the chain no
// longer accepts and drops without an event.
func (s *service) pruneResizes() {
	now := time.Now().Unix()

	for lid, r := range s.resizes {
		if now > r.expiresAt {
			r.cancel()
			delete(s.resizes, lid)
		}
	}
}

// applyResize brings the provider's records in line with a resize the chain
// has applied: the reservation takes the resized group, and a volume lease
// hands its new size to the volume provisioner. The group is read back from
// chain rather than taken from an evaluation, so a resize this run did not
// evaluate, such as an operator's manual accept, is applied the same way.
func (s *service) applyResize(ctx context.Context, lid mtypes.LeaseID) {
	log := s.session.Log().With("lid", lid)

	var group dtypes.Group

	err := retry.Do(func() error {
		res, err := s.session.Client().Query().Deployment().Group(ctx, &dtypes.QueryGroupRequest{ID: lid.GroupID()})
		if err != nil {
			return err
		}

		group = res.GetGroup()

		return nil
	},
		retry.Context(ctx),
		retry.Attempts(resizeApplyAttempts),
		retry.Delay(time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true))
	if err != nil {
		if ctx.Err() == nil {
			log.Error("fetching resized group", "err", err)
		}

		return
	}

	if err := s.cluster.Resize(lid.OrderID(), &group); err != nil {
		log.Error("resizing reservation", "err", err)
	}

	if group.GroupSpec.Volume == nil {
		return
	}

	if err := s.bus.Publish(event.VolumeLeaseResized{LeaseID: lid, Group: &group}); err != nil {
		log.Error("failed to publish to event queue", "err", err)
	}
}
