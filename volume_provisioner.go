package provider

import (
	"context"

	"github.com/boz/go-lifecycle"

	"cosmossdk.io/log"

	mani "pkg.akt.dev/go/manifest/v2beta4"
	"pkg.akt.dev/go/util/pubsub"

	"github.com/akash-network/provider/cluster"
	"github.com/akash-network/provider/event"
	"github.com/akash-network/provider/session"
)

// volumeProvisioner consumes the AEP-87 win handoff. A storage-only (volume)
// lease has no manifest — the on-chain GroupSpec is the whole contract — so
// the bid engine publishes event.VolumeLeaseWon instead of event.LeaseWon and
// this service picks it up.
//
// The win is recorded as a Volume CRD through the cluster client; the storage
// operator reconciles the CRD into a Retain-class PV (operator stage). On
// success the deployment is announced as deployed so the inventory service
// marks the durable reservation allocated — the reservation itself is
// deliberately kept (existing on-win invariant).
type volumeProvisioner struct {
	ctx     context.Context
	session session.Session
	cclient cluster.Client
	bus     pubsub.Bus
	log     log.Logger
	lc      lifecycle.Lifecycle
	sub     pubsub.Subscriber
}

func newVolumeProvisioner(ctx context.Context, clientSession session.Session, bus pubsub.Bus, cclient cluster.Client) (*volumeProvisioner, error) {
	sub, err := bus.Subscribe()
	if err != nil {
		return nil, err
	}

	vp := &volumeProvisioner{
		ctx:     ctx,
		session: clientSession,
		cclient: cclient,
		bus:     bus,
		log:     clientSession.Log().With("cmp", "volume-provisioner"),
		lc:      lifecycle.New(),
		sub:     sub,
	}

	go vp.lc.WatchContext(ctx)
	go vp.run()

	return vp, nil
}

func (vp *volumeProvisioner) Close() error {
	vp.lc.Shutdown(nil)
	return vp.lc.Error()
}

func (vp *volumeProvisioner) run() {
	defer vp.lc.ShutdownCompleted()
	defer vp.sub.Close()

loop:
	for {
		select {
		case err := <-vp.lc.ShutdownRequest():
			vp.lc.ShutdownInitiated(err)
			break loop
		case ev := <-vp.sub.Events():
			switch ev := ev.(type) { // nolint: gocritic
			case event.VolumeLeaseWon:
				vp.log.Info("volume lease won", "lease", ev.LeaseID, "price", ev.Price)

				if err := vp.cclient.DeployVolume(vp.ctx, ev.LeaseID, ev.Group); err != nil {
					// the storage operator's chain re-list heals a missed
					// record; the failure is surfaced, not retried here
					vp.log.Error("recording volume lease", "err", err, "lease", ev.LeaseID)
					break
				}

				// announce the deployment so the inventory service marks
				// the durable reservation allocated (matched by order and
				// group name, like compute deployments)
				if err := vp.bus.Publish(event.ClusterDeployment{
					LeaseID: ev.LeaseID,
					Group: &mani.Group{
						Name: ev.Group.GroupSpec.Name,
					},
					Status: event.ClusterDeploymentDeployed,
				}); err != nil {
					vp.log.Error("failed to publish to event queue", "err", err)
				}
			}
		}
	}
}
