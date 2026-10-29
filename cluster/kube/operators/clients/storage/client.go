package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"

	"cosmossdk.io/log"

	dv1 "pkg.akt.dev/go/node/deployment/v1"

	cstorage "github.com/akash-network/provider/cluster/types/v1beta3/clients/storage"
	clusterutil "github.com/akash-network/provider/cluster/util"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const (
	storageOperatorHealthPath = "health"
)

var (
	errNotAlive           = errors.New("storage operator is not yet alive")
	errStorageOperator    = errors.New("storage operator error")
	errStorageOperatorRPC = fmt.Errorf("%w: remote error", errStorageOperator)
)

// client talks to the storage operator over its REST surface, discovered
// by the standard service labels in akash-services.
type client struct {
	sda    clusterutil.ServiceDiscoveryAgent
	client clusterutil.ServiceClient
	log    log.Logger
	l      sync.Locker
}

var _ cstorage.Client = (*client)(nil)

func NewClient(ctx context.Context, logger log.Logger, endpoint *net.SRV) (cstorage.Client, error) {
	sda, err := clusterutil.NewServiceDiscoveryAgent(ctx, logger, "rest", "operator-storage", "akash-services", endpoint)
	if err != nil {
		return nil, err
	}

	return &client{
		sda: sda,
		log: logger.With("module", "operator-storage-client"),
		l:   &sync.Mutex{},
	}, nil
}

func (sopc *client) String() string {
	return fmt.Sprintf("<%T %p>", sopc, sopc)
}

func (sopc *client) Stop() {
	sopc.sda.Stop()
}

func (sopc *client) Check(ctx context.Context) error {
	req, err := sopc.newRequest(ctx, http.MethodGet, storageOperatorHealthPath, nil)
	if err != nil {
		return err
	}

	response, err := sopc.client.DoRequest(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()

	sopc.log.Info("check result", "status", response.StatusCode)

	if response.StatusCode != http.StatusOK {
		return errNotAlive
	}

	return nil
}

// RetainedVolume implements the adoption bid pre-check. The chain's
// adoption event reaches the storage operator about when the order reaches
// the bid engine, so the volume may already be Adopting, or adopted, for
// the very deployment asking.
func (sopc *client) RetainedVolume(ctx context.Context, owner string, vid string, dseq uint64) (bool, error) {
	state, found, err := sopc.volumeState(ctx, owner, vid)
	if err != nil || !found {
		return false, err
	}

	// a copy that never finished arriving is no volume to adopt, nor is a
	// replica whose copy of its primary is missing or half written
	if state.ImportFrom != "" || state.Syncing {
		return false, nil
	}

	switch crd.VolumePhase(state.Phase) {
	case crd.VolumePhaseRetained:
		return true, nil
	case crd.VolumePhaseAdopting, crd.VolumePhaseProvisioned:
		return state.DSeq == strconv.FormatUint(dseq, 10), nil
	default:
		return false, nil
	}
}

// ParkedVolume implements the attach bid pre-check. Provisioned is the
// parked phase: PV held by the holder claim, no attachment.
func (sopc *client) ParkedVolume(ctx context.Context, ref dv1.VolumeRef) (bool, error) {
	state, found, err := sopc.volumeState(ctx, ref.Owner, ref.Name)
	if err != nil || !found {
		return false, err
	}

	return state.Phase == string(crd.VolumePhaseProvisioned) && state.AttachedLease == "", nil
}

func (sopc *client) volumeState(ctx context.Context, owner string, vid string) (cstorage.VolumeState, bool, error) {
	path := fmt.Sprintf("volume/%s/%s", owner, vid)

	req, err := sopc.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return cstorage.VolumeState{}, false, err
	}

	response, err := sopc.client.DoRequest(req)
	if err != nil {
		return cstorage.VolumeState{}, false, err
	}
	// the service client allows two connections per host, and an unclosed
	// body pins one of them: the third lookup would block forever
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusNotFound {
		return cstorage.VolumeState{}, false, nil
	}

	if response.StatusCode != http.StatusOK {
		return cstorage.VolumeState{}, false, fmt.Errorf("%w: status %d", errStorageOperatorRPC, response.StatusCode)
	}

	var state cstorage.VolumeState
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		return cstorage.VolumeState{}, false, err
	}

	return state, true, nil
}

func (sopc *client) newRequest(ctx context.Context, method string, path string, body io.Reader) (*http.Request, error) {
	sopc.l.Lock()
	defer sopc.l.Unlock()

	if sopc.client == nil {
		var err error
		sopc.client, err = sopc.sda.GetClient(ctx, false, false)
		if err != nil {
			return nil, err
		}
	}

	return sopc.client.CreateRequest(ctx, method, path, body)
}
