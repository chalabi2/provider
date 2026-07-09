package provider

import (
	"context"

	"github.com/boz/go-lifecycle"

	"cosmossdk.io/log"

	"pkg.akt.dev/go/util/pubsub"

	"github.com/akash-network/provider/event"
	"github.com/akash-network/provider/session"
)

// volumeProvisioner consumes the AEP-87 win handoff. A storage-only (volume)
// lease has no manifest — the on-chain GroupSpec is the whole contract — so
// the bid engine publishes event.VolumeLeaseWon instead of event.LeaseWon and
// this service picks it up.
//
// This is the provisioner stub: it acknowledges the event so won volume
// leases are visibly handled. The storage operator client replaces the log
// line with Volume CRD creation (provision the retain-class PV and park it)
// when the operator stage lands.
type volumeProvisioner struct {
	session session.Session
	log     log.Logger
	lc      lifecycle.Lifecycle
	sub     pubsub.Subscriber
}

func newVolumeProvisioner(ctx context.Context, clientSession session.Session, bus pubsub.Bus) (*volumeProvisioner, error) {
	sub, err := bus.Subscribe()
	if err != nil {
		return nil, err
	}

	vp := &volumeProvisioner{
		session: clientSession,
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
				// TODO(aep-87): create the Volume CRD via the storage
				// operator client (operator stage); the reservation is
				// deliberately kept (existing on-win invariant).
				vp.log.Info("volume lease won; awaiting storage operator provisioner",
					"lease", ev.LeaseID, "price", ev.Price)
			}
		}
	}
}
