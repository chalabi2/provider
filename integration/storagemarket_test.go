//go:build e2e

// Package integration: AEP-87 decoupled storage market provider e2e
// (IMPLEMENTATION.md §8.2-§8.3), modeled on E2EPersistentStorageDefault.
//
// How to run (requires docker + kind on the host):
//
//	cd _run/kube && direnv exec . make kube-cluster-setup-e2e && cd ../..
//	direnv exec . make test-e2e-integration
//
// or just these suites:
//
//	KUBE_INGRESS_IP=127.0.0.1 KUBE_INGRESS_PORT=10080 TEST_INTEGRATION=true \
//	  go test -count=1 -tags e2e -v ./integration/... \
//	  -run 'TestIntegrationTestSuite/(E2EStorageMarket|E2EStorageMarketMigration)' \
//	  -timeout 5400s
//
// (TestIntegrationTestSuite drives every suite; budget the -timeout
// accordingly - the money path alone waits out a real escrow runway plus
// the 1m withdrawal cadence.)
//
// Cluster prerequisites, all installed by kube-cluster-setup-e2e:
//   - beta3 (Delete) and beta3-retain (Retain, Immediate binding)
//     rancher.io/local-path StorageClasses (_docs/kustomize/storage/)
//   - the Volume CRD (pkg/apis/akash.network/crd.yaml)
//   - the akash-volumes namespace (_docs/kustomize/networking/)
//   - for the two-provider migration suite: the local-path provisioner
//     repointed at /tmp/akash-e2e-storage with the identical-path kind
//     extraMount (_run/kube/kind-config.yaml + script/setup-kube.sh), so
//     the in-process replication file driver can read the source PV and
//     land the import in the destination PV from the host.
//
// What is proven where (honest labeling per IMPLEMENTATION.md §8.3):
// the lifecycle and money-path suites are fully in-cluster - the UUID
// rides the PV through re-park, re-attach, exhaustion, retention, and
// adoption. The migration suite proves the chain coordination (reclaim ->
// Exporting freeze -> auto-re-order -> destination lease) and the
// transfer plumbing (mTLS x/cert identities, chain-derived authorization,
// chunked verified export/import) over the REAL provider gateways; the
// transferred payload is the volume's image file staged on the source PV
// (the file driver's unit of transfer - on RBD the driver exports the
// real block image, validated out-of-band on a Rook pair per the
// runbook). rbd/rbd-mirror behavior is NOT covered here by design.
package integration

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sdkmath "cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"pkg.akt.dev/go/cli"
	clitestutil "pkg.akt.dev/go/cli/testutil"
	aclient "pkg.akt.dev/go/node/client/discovery"
	dtypes "pkg.akt.dev/go/node/deployment/v1"
	dvbeta "pkg.akt.dev/go/node/deployment/v1beta5"
	mtypes "pkg.akt.dev/go/node/market/v1"
	mvbeta "pkg.akt.dev/go/node/market/v2beta1"
	"pkg.akt.dev/go/sdkutil"
	atls "pkg.akt.dev/go/util/tls"
	testnet "pkg.akt.dev/node/v2/testutil/network"

	pclient "github.com/akash-network/provider/client"
	pcmd "github.com/akash-network/provider/cmd/provider-services/cmd"
	operatorcommon "github.com/akash-network/provider/operator/common"
	"github.com/akash-network/provider/operator/storage/replication"
	crd "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	ptestutil "github.com/akash-network/provider/testutil/provider"
)

const (
	// storageReclamationFloor is the genesis MinVolumeReclamationWindow -
	// production ships 24h; the suite needs a floor a test can wait out.
	storageReclamationFloor = 5 * time.Second

	// storageReclamationWindow is the window the providers offer in bids
	// (>= the floor, and >= the genesis MinReclamationWindow of 1s).
	storageReclamationWindow = 15 * time.Second

	// storageVolClass is the sellable class volume orders are placed in;
	// beta3-retain is its operator-managed Retain twin.
	storageVolClass = "beta3"

	// storageManifestNS / storageVolumesNS are provider A's namespaces
	// (the shared-suite defaults).
	storageManifestNS = "lease"
	storageVolumesNS  = "akash-volumes"

	// storageManifestNSB / storageVolumesNSB isolate provider B's CRDs in
	// the shared kind cluster - two providers of the same volume derive
	// the same CRD name, which only separate clusters (production) or
	// separate namespaces (this harness) keep apart.
	storageManifestNSB = "leaseb"
	storageVolumesNSB  = "akash-volumes-b"
)

// sdlVolumeTemplate is the volume deployment: a single beta3 volume,
// retain lifecycle, 1h retention (long enough to adopt inside the test,
// short enough that leaked volumes age out of dev clusters).
// args: vid, deposit-irrelevant, priced in uact.
const sdlVolumeTemplate = `---
version: "2.2"

volumes:
  %[1]s:
    size: 1Gi
    class: beta3
    lifecycle:
      reclaim: retain
      retention: 1h

profiles:
  placement:
    global:
      pricing:
        %[1]s:
          denom: uact
          amount: 150

deployment:
  %[1]s:
    global:
      profile: %[1]s
      count: 1
`

// sdlVolumeAdoptTemplate re-mints the vid by adopting the dead volume
// deployment's data. args: vid, dead dseq.
const sdlVolumeAdoptTemplate = `---
version: "2.2"

volumes:
  %[1]s:
    size: 1Gi
    class: beta3
    lifecycle:
      reclaim: retain
      retention: 1h
    adopt:
      dseq: %[2]d

profiles:
  placement:
    global:
      pricing:
        %[1]s:
          denom: uact
          amount: 150

deployment:
  %[1]s:
    global:
      profile: %[1]s
      count: 1
`

// sdlComputeAttachTemplate is the attaching compute deployment: the
// e2e-test app with the volume mounted at its data directory.
// args: vid, volume owner, volume dseq, ingress host.
const sdlComputeAttachTemplate = `---
version: "2.2"

volumes:
  %[1]s:
    external:
      owner: %[2]s
      dseq: %[3]d

services:
  web:
    image: ovrclk/e2e-test
    expose:
      - port: 8080
        as: 80
        to:
          - global: true
        accept:
          - %[4]s
    params:
      storage:
        data:
          mount: /var/lib/e2e-test
          volume: %[1]s

profiles:
  compute:
    web:
      resources:
        cpu:
          units: "0.01"
        memory:
          size: "128Mi"
        storage: []
  placement:
    global:
      pricing:
        web:
          denom: uact
          amount: 100

deployment:
  web:
    global:
      profile: web
      count: 1
`

// E2EStorageMarket drives the single-provider AEP-87 flows: the core
// volume lifecycle and the escrow money path.
type E2EStorageMarket struct {
	IntegrationTestSuite
}

// E2EStorageMarketMigration adds a second in-process provider against the
// same kind cluster and drives reclaim -> VolumeTransfer -> re-attach. It
// embeds the base suite directly (not E2EStorageMarket) so the lifecycle
// and money-path tests do not re-run here.
type E2EStorageMarketMigration struct {
	IntegrationTestSuite

	addrProviderB        sdk.AccAddress
	grpcHostProviderB    string
	storageOperatorHostB string
}

// the storage suites share the volume-aware teardown
func (s *E2EStorageMarket) TearDownTest()  { s.storageTearDownTest() }
func (s *E2EStorageMarket) TearDownSuite() { s.storageTearDownSuite() }

func (s *E2EStorageMarketMigration) TearDownTest()  { s.storageTearDownTest() }
func (s *E2EStorageMarketMigration) TearDownSuite() { s.storageTearDownSuite() }

// ===========================
// suite helpers
// ===========================

// writeSDL lands a rendered SDL in the network's scratch dir.
func (s *IntegrationTestSuite) writeSDL(name, content string) string {
	path := filepath.Join(s.network.BaseDir, name)
	s.Require().NoError(os.WriteFile(path, []byte(content), 0o600))

	return path
}

// createVolume broadcasts a volume deployment via the volume tx CLI and
// returns its deployment ID.
func (s *IntegrationTestSuite) createVolume(sdlPath string, dseq uint64, deposit string) dtypes.DeploymentID {
	res, err := clitestutil.ExecTestCLICmd(
		s.ctx,
		s.cctx,
		cli.GetTxVolumeCreateCmd(),
		cli.TestFlags().
			With(sdlPath).
			WithFrom(s.addrTenant.String()).
			WithDSeq(dseq).
			With(fmt.Sprintf("--deposit=%s", deposit)).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))
	clitestutil.ValidateTxSuccessful(s.ctx, s.T(), s.cctx, res.Bytes())

	return dtypes.DeploymentID{Owner: s.addrTenant.String(), DSeq: dseq}
}

// createComputeDeployment broadcasts an attaching compute deployment,
// funded well past every runway this suite waits out.
func (s *IntegrationTestSuite) createComputeDeployment(sdlPath string, dseq uint64) dtypes.DeploymentID {
	res, err := clitestutil.ExecDeploymentCreate(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(sdlPath).
			WithFrom(s.addrTenant.String()).
			WithDSeq(dseq).
			With(deploymentUactDeposit).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))
	clitestutil.ValidateTxSuccessful(s.ctx, s.T(), s.cctx, res.Bytes())

	return dtypes.DeploymentID{Owner: s.addrTenant.String(), DSeq: dseq}
}

// waitForBid polls the bid query until the provider's bid exists - the
// deterministic replacement for racing the event stream.
func (s *IntegrationTestSuite) waitForBid(bidID mtypes.BidID) {
	s.T().Helper()

	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, err := clitestutil.ExecQueryBid(s.ctx, s.cctx, cli.TestFlags().WithBidID(bidID)...)
		if err == nil {
			return
		}

		if time.Now().After(deadline) {
			s.Require().FailNowf("bid never arrived", "bid %v: %v", bidID, err)
		}

		time.Sleep(2 * time.Second)
	}
}

// createLease accepts the given bid.
func (s *IntegrationTestSuite) createLease(bidID mtypes.BidID) mtypes.LeaseID {
	res, err := clitestutil.ExecCreateLease(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			WithBidID(bidID).
			WithFrom(s.addrTenant.String()).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))
	clitestutil.ValidateTxSuccessful(s.ctx, s.T(), s.cctx, res.Bytes())

	return bidID.LeaseID()
}

// queryLease returns the lease's current chain state.
func (s *IntegrationTestSuite) queryLease(lid mtypes.LeaseID) mtypes.Lease {
	resp, err := clitestutil.ExecQueryLease(
		s.ctx,
		s.cctx,
		cli.TestFlags().WithBidID(lid.BidID()).WithOutputJSON()...,
	)
	s.Require().NoError(err)

	out := &mvbeta.QueryLeaseResponse{}
	s.Require().NoError(s.cctx.Codec.UnmarshalJSON(resp.Bytes(), out))

	return out.Lease
}

// waitForLeaseState polls until the lease reaches one of the wanted
// states.
func (s *IntegrationTestSuite) waitForLeaseState(lid mtypes.LeaseID, timeout time.Duration, want ...mtypes.Lease_State) mtypes.Lease {
	s.T().Helper()

	deadline := time.Now().Add(timeout)
	for {
		lease := s.queryLease(lid)
		for _, state := range want {
			if lease.State == state {
				return lease
			}
		}

		if time.Now().After(deadline) {
			s.Require().FailNowf("lease state never reached", "lease %v: state %v, wanted %v", lid, lease.State, want)
		}

		time.Sleep(3 * time.Second)
	}
}

// waitForVolume polls the Volume CRD until check passes.
func (s *IntegrationTestSuite) waitForVolume(ns, name string, timeout time.Duration, desc string, check func(*crd.Volume) bool) *crd.Volume {
	s.T().Helper()

	deadline := time.Now().Add(timeout)
	for {
		vol, err := s.akashClient.AkashV2beta2().Volumes(ns).Get(s.ctx, name, metav1.GetOptions{})
		if err == nil && check(vol) {
			return vol
		}

		if time.Now().After(deadline) {
			phase := "<missing>"
			if err == nil {
				phase = string(vol.Status.Phase)
			}
			s.Require().FailNowf("volume CRD never converged", "%s/%s: waiting for %s, phase %s, err %v", ns, name, desc, phase, err)
		}

		time.Sleep(2 * time.Second)
	}
}

// volumePVHostPath resolves the CRD's backing PV to its host path. The
// two-provider migration flow requires it to exist on the host (the
// identical-path kind mount).
func (s *IntegrationTestSuite) volumePVHostPath(ns, name string) string {
	s.T().Helper()

	vol := s.waitForVolume(ns, name, time.Minute, "a bound PV", func(v *crd.Volume) bool {
		return v.Status.PVName != ""
	})

	pv, err := s.kubeClient.CoreV1().PersistentVolumes().Get(s.ctx, vol.Status.PVName, metav1.GetOptions{})
	s.Require().NoError(err)

	switch {
	case pv.Spec.HostPath != nil:
		return pv.Spec.HostPath.Path
	case pv.Spec.Local != nil:
		return pv.Spec.Local.Path
	default:
		s.Require().FailNowf("unexpected PV backend", "pv %s is neither hostPath nor local", pv.Name)
		return ""
	}
}

// closeVolumeDeployment drives the volume deployment's terminal close
// through the reclamation window: the first close starts the window, the
// second (past the deadline) completes under the retain reason.
func (s *IntegrationTestSuite) closeVolumeDeployment(dseq uint64) {
	s.T().Helper()

	for i := 0; i < 2; i++ {
		res, err := clitestutil.ExecTestCLICmd(
			s.ctx,
			s.cctx,
			cli.GetTxVolumeCloseCmd(),
			cli.TestFlags().
				WithFrom(s.addrTenant.String()).
				WithDSeq(dseq).
				Append(cliFlags)...,
		)
		s.Require().NoError(err)
		s.Require().NoError(s.waitForBlocksCommitted(1))
		clitestutil.ValidateTxSuccessful(s.ctx, s.T(), s.cctx, res.Bytes())

		if i == 0 {
			time.Sleep(storageReclamationWindow + storageReclamationFloor)
		}
	}
}

// closeComputeDeployment closes a compute deployment (immediate - the
// reclamation routing applies to volume groups only).
func (s *IntegrationTestSuite) closeComputeDeployment(dseq uint64) {
	s.T().Helper()

	res, err := clitestutil.ExecDeploymentClose(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			WithFrom(s.addrTenant.String()).
			WithOwner(s.addrTenant.String()).
			WithDSeq(dseq).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(1))
	clitestutil.ValidateTxSuccessful(s.ctx, s.T(), s.cctx, res.Bytes())
}

// appWrite stores value under key through the deployed e2e-test app.
func (s *IntegrationTestSuite) appWrite(host, key, value string) {
	s.T().Helper()

	appURL := fmt.Sprintf("http://%s:%s/SET/%s", s.appHost, s.appPort, key)
	httpResp := queryAppWithRetries(s.T(), appURL, host, 120, queryWithBody([]byte(value)))
	s.Require().Equal(http.StatusOK, httpResp.StatusCode)
}

// appRead returns the app's value under key.
func (s *IntegrationTestSuite) appRead(host, key string) string {
	s.T().Helper()

	appURL := fmt.Sprintf("http://%s:%s/GET/%s", s.appHost, s.appPort, key)
	httpResp := queryAppWithRetries(s.T(), appURL, host, 120)
	s.Require().Equal(http.StatusOK, httpResp.StatusCode)

	bodyData, err := io.ReadAll(httpResp.Body)
	s.Require().NoError(err)

	return string(bodyData)
}

// ===========================
// teardown: volume-aware cleanup
// ===========================

// TearDownTest overrides the base close-everything sweep: volume
// deployments only close through the reclamation window, and attached
// volumes refuse to close at all, so computes go first and volumes are
// double-closed. Best effort - tests are expected to close their own
// deployments; this heals failures.
func (s *IntegrationTestSuite) storageTearDownTest() {
	s.T().Log("cleaning up after storage-market e2e test")

	resp, err := clitestutil.ExecQueryDeployments(s.ctx, s.cctx, cli.TestFlags().WithOutputJSON()...)
	s.Require().NoError(err)

	deployResp := &dvbeta.QueryDeploymentsResponse{}
	s.Require().NoError(s.cctx.Codec.UnmarshalJSON(resp.Bytes(), deployResp))

	var volumes []dvbeta.QueryDeploymentResponse

	for _, dep := range deployResp.Deployments {
		if dep.Deployment.State != dtypes.DeploymentActive {
			continue
		}

		if len(dep.Groups) > 0 && dep.Groups[0].GroupSpec.Volume != nil {
			volumes = append(volumes, dep)
			continue
		}

		_, err := clitestutil.ExecDeploymentClose(
			s.ctx,
			s.cctx,
			cli.TestFlags().
				WithFrom(s.addrTenant.String()).
				WithOwner(dep.Groups[0].ID.Owner).
				WithDSeq(dep.Deployment.ID.DSeq).
				Append(cliFlags)...,
		)
		if err != nil {
			s.T().Logf("teardown: closing compute deployment %d: %v", dep.Deployment.ID.DSeq, err)
		}
	}

	if len(volumes) == 0 {
		return
	}

	_ = s.waitForBlocksCommitted(1)

	// first pass starts (or restarts) the reclamation windows
	closeVolume := func(dseq uint64) {
		_, err := clitestutil.ExecTestCLICmd(
			s.ctx,
			s.cctx,
			cli.GetTxVolumeCloseCmd(),
			cli.TestFlags().
				WithFrom(s.addrTenant.String()).
				WithDSeq(dseq).
				Append(cliFlags)...,
		)
		if err != nil {
			s.T().Logf("teardown: closing volume deployment %d: %v", dseq, err)
		}
	}

	for _, dep := range volumes {
		closeVolume(dep.Deployment.ID.DSeq)
	}

	time.Sleep(storageReclamationWindow + storageReclamationFloor)

	for _, dep := range volumes {
		closeVolume(dep.Deployment.ID.DSeq)
	}

	_ = s.waitForBlocksCommitted(1)
}

// TearDownSuite purges the kube leftovers the storage flows create -
// Volume CRDs, parked holder PVCs, and Retain PVs survive chain teardown
// by design and would otherwise leak across suite runs on the shared kind
// cluster.
func (s *IntegrationTestSuite) storageTearDownSuite() {
	s.T().Log("cleaning up storage-market kube state")

	propagation := metav1.DeletePropagationForeground
	delOpts := metav1.DeleteOptions{PropagationPolicy: &propagation}

	for _, ns := range []string{storageManifestNS, storageManifestNSB} {
		err := s.akashClient.AkashV2beta2().Volumes(ns).DeleteCollection(s.ctx, delOpts, metav1.ListOptions{})
		if err != nil && !kerrors.IsNotFound(err) {
			s.T().Logf("teardown: deleting volume CRDs in %s: %v", ns, err)
		}
	}

	for _, ns := range []string{storageVolumesNS, storageVolumesNSB} {
		err := s.kubeClient.CoreV1().PersistentVolumeClaims(ns).DeleteCollection(s.ctx, delOpts, metav1.ListOptions{})
		if err != nil && !kerrors.IsNotFound(err) {
			s.T().Logf("teardown: deleting holder PVCs in %s: %v", ns, err)
		}
	}

	err := s.kubeClient.CoreV1().PersistentVolumes().DeleteCollection(s.ctx, delOpts, metav1.ListOptions{
		LabelSelector: "akash.network/component=volume",
	})
	if err != nil && !kerrors.IsNotFound(err) {
		s.T().Logf("teardown: deleting volume PVs: %v", err)
	}

	s.TearDownSuite()
}

// ===========================
// core lifecycle
// ===========================

// TestLifecycleCore is the §8.2 core loop: create volume -> bid -> lease
// -> attach compute -> write UUID -> close compute deployment -> PV
// re-parks while the volume lease stays active -> second compute
// deployment against the same ref -> read the UUID back.
func (s *E2EStorageMarket) TestLifecycleCore() {
	const (
		vid          = "e2e-data"
		volDSeq      = uint64(200)
		computeDSeq  = uint64(201)
		compute2DSeq = uint64(202)
		host1        = "storagemarket.localhost"
		host2        = "storagemarket2.localhost"
	)

	tenant := s.addrTenant.String()
	volName := crd.VolumeName(tenant, vid)

	// volume: create -> bid -> lease
	volSDL := s.writeSDL("volume-core.yaml", fmt.Sprintf(sdlVolumeTemplate, vid))
	volID := s.createVolume(volSDL, volDSeq, uactMinDeposit)

	volBid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(volID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(volBid)
	volLease := s.createLease(volBid)

	// the operator provisions the Retain PV and parks it
	s.waitForVolume(storageManifestNS, volName, 2*time.Minute, "phase Provisioned", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.PVName != ""
	})

	// attach compute: bid gated to the colocated provider, manifest
	// deploys the app against the pre-bound claim
	attachSDL := s.writeSDL("attach-core.yaml", fmt.Sprintf(sdlComputeAttachTemplate, vid, tenant, volDSeq, host1))
	computeID := s.createComputeDeployment(attachSDL, computeDSeq)

	computeBid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(computeID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(computeBid)
	computeLease := s.createLease(computeBid)

	_, err := ptestutil.ExecSendManifest(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(attachSDL).
			WithHome(s.validator.ClientCtx.HomeDir).
			WithFrom(tenant).
			WithDSeq(computeLease.DSeq).
			WithOutputJSON()...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))

	attached := s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "phase Attached", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseAttached
	})
	pvName := attached.Status.PVName

	// write the UUID through the app
	testData := uuid.New().String()
	s.appWrite(host1, "value", testData)
	s.Require().Equal(testData, s.appRead(host1, "value"))

	// close the compute deployment: the namespace (and its attach PVC)
	// goes away, the Retain PV survives, the operator re-parks it, and
	// the volume lease is untouched
	s.closeComputeDeployment(computeDSeq)

	reparked := s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "re-parked (Provisioned, no attachment)", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.AttachedLease == ""
	})
	s.Require().Equal(pvName, reparked.Status.PVName, "the volume must keep its PV across re-parking")

	volLeaseState := s.queryLease(volLease)
	s.Require().Equal(mtypes.LeaseActive, volLeaseState.State, "the volume lease must survive the compute close")

	// second compute deployment against the same ref reads the UUID back
	attach2SDL := s.writeSDL("attach2-core.yaml", fmt.Sprintf(sdlComputeAttachTemplate, vid, tenant, volDSeq, host2))
	compute2ID := s.createComputeDeployment(attach2SDL, compute2DSeq)

	compute2Bid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(compute2ID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(compute2Bid)
	compute2Lease := s.createLease(compute2Bid)

	_, err = ptestutil.ExecSendManifest(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(attach2SDL).
			WithHome(s.validator.ClientCtx.HomeDir).
			WithFrom(tenant).
			WithDSeq(compute2Lease.DSeq).
			WithOutputJSON()...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))

	s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "phase Attached (second lease)", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseAttached
	})

	s.Require().Equal(testData, s.appRead(host2, "value"), "the UUID must survive re-parking and re-attachment")

	// orderly close: compute first, then the volume through its window
	s.closeComputeDeployment(compute2DSeq)
	s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "re-parked before volume close", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.AttachedLease == ""
	})
	s.closeVolumeDeployment(volDSeq)
}

// ===========================
// money path
// ===========================

// TestMoneyPathExhaustionAdoption is the §8.2 money path: a tiny-deposit
// volume exhausts its escrow -> the withdrawal-driven settle cascades
// (cascade-detach, volume_unfunded) -> the CRD retains the data -> an
// adoption deployment re-binds it -> a fresh compute deployment reads the
// UUID back.
func (s *E2EStorageMarket) TestMoneyPathExhaustionAdoption() {
	const (
		vid          = "e2e-money"
		volDSeq      = uint64(210)
		computeDSeq  = uint64(211)
		adoptDSeq    = uint64(212)
		compute2DSeq = uint64(213)
		host1        = "storagemoney.localhost"
		host2        = "storagemoney2.localhost"

		// ~150 blocks of runway at the 1uact/block volume rate: enough to
		// attach and write the UUID, small enough to exhaust in test time
		tinyDeposit = "150uact"
	)

	tenant := s.addrTenant.String()
	volName := crd.VolumeName(tenant, vid)

	// tiny-deposit volume
	volSDL := s.writeSDL("volume-money.yaml", fmt.Sprintf(sdlVolumeTemplate, vid))
	volID := s.createVolume(volSDL, volDSeq, tinyDeposit)

	volBid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(volID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(volBid)
	volLease := s.createLease(volBid)

	s.waitForVolume(storageManifestNS, volName, 2*time.Minute, "phase Provisioned", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.PVName != ""
	})

	// attach and write the UUID while the runway lasts
	attachSDL := s.writeSDL("attach-money.yaml", fmt.Sprintf(sdlComputeAttachTemplate, vid, tenant, volDSeq, host1))
	computeID := s.createComputeDeployment(attachSDL, computeDSeq)

	computeBid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(computeID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(computeBid)
	computeLease := s.createLease(computeBid)

	_, err := ptestutil.ExecSendManifest(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(attachSDL).
			WithHome(s.validator.ClientCtx.HomeDir).
			WithFrom(tenant).
			WithDSeq(computeLease.DSeq).
			WithOutputJSON()...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))

	testData := uuid.New().String()
	s.appWrite(host1, "value", testData)
	s.Require().Equal(testData, s.appRead(host1, "value"))

	// exhaustion: the provider's scheduled withdrawal is the detector -
	// the settle overdraws the volume account and the escrow cascade
	// closes the volume lease as unfunded and force-detaches the compute
	// lease. Runway (~150 blocks) + withdrawal cadence (1m) bounds the
	// wait.
	volLeaseState := s.waitForLeaseState(volLease, 10*time.Minute, mtypes.LeaseInsufficientFunds)
	s.Require().Equal(mtypes.LeaseClosedReasonVolumeUnfunded, volLeaseState.Reason, "exhaustion must close as volume_unfunded - the diagnosable adoption trigger")

	computeLeaseState := s.waitForLeaseState(computeLease, 2*time.Minute, mtypes.LeaseClosed)
	s.Require().Equal(mtypes.LeaseClosedReasonVolumeDetach, computeLeaseState.Reason, "the attached compute lease must cascade-detach")

	// the operator retains the data: phase Retained, GC clock stamped
	retained := s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "phase Retained", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseRetained
	})
	s.Require().NotNil(retained.Status.RetainedUntil, "retention must carry the chain-computable GC deadline")
	pvName := retained.Status.PVName

	// the cascade re-orders the detached compute group; it is not wanted
	// here - close the deployment
	s.closeComputeDeployment(computeDSeq)

	// adoption: a new volume deployment adopting the dead group. The
	// chain gates its bids to the retaining provider; the operator
	// verifies the chain policy and re-parks the PV under the new
	// identity.
	adoptSDL := s.writeSDL("volume-adopt.yaml", fmt.Sprintf(sdlVolumeAdoptTemplate, vid, volDSeq))
	adoptID := s.createVolume(adoptSDL, adoptDSeq, uactMinDeposit)

	adoptBid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(adoptID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(adoptBid)
	s.createLease(adoptBid)

	adopted := s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "re-parked under the adopting group", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned &&
			v.Spec.GroupID.DSeq == fmt.Sprintf("%d", adoptDSeq)
	})
	s.Require().Equal(pvName, adopted.Status.PVName, "adoption must re-bind the same PV - data identity survives, market identity does not")

	// re-attach against the adopting deployment and read the UUID back
	attach2SDL := s.writeSDL("attach2-money.yaml", fmt.Sprintf(sdlComputeAttachTemplate, vid, tenant, adoptDSeq, host2))
	compute2ID := s.createComputeDeployment(attach2SDL, compute2DSeq)

	compute2Bid := mtypes.MakeBidID(
		mtypes.MakeOrderID(dtypes.MakeGroupID(compute2ID, 1), 1),
		s.addrProvider,
	)
	s.waitForBid(compute2Bid)
	compute2Lease := s.createLease(compute2Bid)

	_, err = ptestutil.ExecSendManifest(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(attach2SDL).
			WithHome(s.validator.ClientCtx.HomeDir).
			WithFrom(tenant).
			WithDSeq(compute2Lease.DSeq).
			WithOutputJSON()...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))

	s.Require().Equal(testData, s.appRead(host2, "value"), "the UUID must survive exhaustion, retention, and adoption")

	// orderly close
	s.closeComputeDeployment(compute2DSeq)
	s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "re-parked before adoption close", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.AttachedLease == ""
	})
	s.closeVolumeDeployment(adoptDSeq)
}

// ===========================
// two-provider migration
// ===========================

// SetupSuite brings up the shared harness, then a full second provider
// (distinct keys, ports, and CRD namespaces) against the same cluster -
// the cheapest approximation of two providers the harness supports.
func (s *E2EStorageMarketMigration) SetupSuite() {
	s.IntegrationTestSuite.SetupSuite()
	s.setupProviderB()
}

func (s *E2EStorageMarketMigration) setupProviderB() {
	cctx := s.cctx

	// provider B's namespaces in the shared cluster
	for _, ns := range []string{storageManifestNSB, storageVolumesNSB} {
		_, err := s.kubeClient.CoreV1().Namespaces().Create(s.ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   ns,
				Labels: map[string]string{"akash.network": "true"},
			},
		}, metav1.CreateOptions{})
		if err != nil && !kerrors.IsAlreadyExists(err) {
			s.Require().NoError(err)
		}
	}

	// keys and funding
	kb := cctx.Keyring
	_, _, err := kb.NewMnemonic("keyFooB", keyring.English, sdk.FullFundraiserPath, "", hd.Secp256k1)
	s.Require().NoError(err)

	keyProviderB, err := kb.Key("keyFooB")
	s.Require().NoError(err)

	s.addrProviderB, err = keyProviderB.GetAddress()
	s.Require().NoError(err)

	sendTokens := sdk.Coins{
		sdk.NewCoin(s.cfg.BondDenom, mvbeta.DefaultBidMinDeposit.Amount.MulRaw(12)),
		sdk.NewCoin(sdkutil.DenomUact, sdkmath.NewInt(uactMinDepositAmount*12)),
	}

	res, err := clitestutil.ExecSend(
		s.ctx,
		cctx,
		cli.TestFlags().
			With(
				s.addrProviderB.String(),
				sendTokens.String()).
			WithFrom(s.validator.Address.String()).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.network.WaitForNextBlock())
	clitestutil.ValidateTxSuccessful(s.ctx, s.T(), cctx, res.Bytes())

	// ports: gateway REST, hostname operator, storage operator, gateway gRPC
	ports, err := testnet.GetFreePorts(4)
	s.Require().NoError(err)

	provBHost := fmt.Sprintf("localhost:%d", ports[0])
	hostnameOperatorBHost := fmt.Sprintf("localhost:%d", ports[1])
	s.storageOperatorHostB = fmt.Sprintf("localhost:%d", ports[2])
	s.grpcHostProviderB = fmt.Sprintf("localhost:%d", ports[3])

	// on-chain provider record
	provFileStr := fmt.Sprintf(providerTemplate, "https://"+provBHost)
	provPath := filepath.Join(s.network.BaseDir, "provider-b.yaml")
	s.Require().NoError(os.WriteFile(provPath, []byte(provFileStr), 0o600))

	res, err = clitestutil.ExecTxCreateProvider(
		s.ctx,
		cctx,
		cli.TestFlags().
			With(provPath).
			WithFrom(s.addrProviderB.String()).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.network.WaitForNextBlock())
	clitestutil.ValidateTxSuccessful(s.ctx, s.T(), cctx, res.Bytes())

	// x/cert server certificate - also provider B's mTLS identity on the
	// VolumeTransfer data plane
	_, err = clitestutil.TxGenerateServerExec(
		s.ctx,
		cctx,
		cli.TestFlags().
			With("localhost").
			WithFrom(s.addrProviderB.String()).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)

	_, err = clitestutil.TxPublishServerExec(
		s.ctx,
		cctx,
		cli.TestFlags().
			WithFrom(s.addrProviderB.String()).
			Append(cliFlags)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.network.WaitForNextBlock())

	dialer := net.Dialer{
		Timeout: time.Second * 3,
	}

	// hostname operator B watches provider B's manifest namespace
	s.group.Go(func() error {
		s.T().Logf("starting hostname operator B on %s", hostnameOperatorBHost)

		_, err := ptestutil.RunLocalOperator(
			s.ctx,
			cctx,
			cli.TestFlags().
				With("hostname").
				WithFlag(operatorcommon.FlagRESTAddress, "127.0.0.1").
				WithFlag(operatorcommon.FlagRESTPort, ports[1]).
				WithFlag("k8s-manifest-ns", storageManifestNSB)...,
		)
		s.Assert().NoError(err)
		return err
	})
	waitForTCPSocket(s.ctx, dialer, hostnameOperatorBHost, s.T())

	// storage operator B
	s.group.Go(func() error {
		s.T().Logf("starting storage operator B on %s", s.storageOperatorHostB)

		_, err := ptestutil.RunLocalOperator(
			s.ctx,
			cctx,
			cli.TestFlags().
				With("storage").
				WithFlag(operatorcommon.FlagRESTAddress, "127.0.0.1").
				WithFlag(operatorcommon.FlagRESTPort, ports[2]).
				WithFlag("node", s.validator.RPCAddress).
				WithFlag("resync-interval", "5s").
				WithFlag("k8s-manifest-ns", storageManifestNSB).
				WithFlag("volumes-namespace", storageVolumesNSB).
				WithProvider(s.addrProviderB.String())...,
		)
		s.Assert().NoError(err)
		return err
	})
	waitForTCPSocket(s.ctx, dialer, s.storageOperatorHostB, s.T())

	// provider B daemon
	pArgsB := cli.TestFlags().
		WithHome(s.cliHome).
		WithFrom(s.addrProviderB.String()).
		WithGasAuto().
		WithFlag(pcmd.FlagClusterK8s, true).
		WithFlag(pcmd.FlagGatewayListenAddress, provBHost).
		WithFlag(pcmd.FlagClusterPublicHostname, ptestutil.TestClusterPublicHostname).
		WithFlag(pcmd.FlagClusterNodePortQuantity, ptestutil.TestClusterNodePortQuantity).
		WithFlag(pcmd.FlagPersistentConfigBackend, "memory").
		WithFlag("deployment-runtime-class", "none").
		WithFlag("hostname-operator-endpoint", hostnameOperatorBHost).
		WithFlag(pcmd.FlagBidPricingStrategy, "randomRange").
		WithFlag("k8s-manifest-ns", storageManifestNSB).
		WithFlag("volume-classes", storageVolClass).
		WithFlag("volume-max-retention", "48h").
		WithFlag("storage-operator-endpoint", s.storageOperatorHostB).
		WithFlag(pcmd.FlagGatewayGRPCListenAddress, s.grpcHostProviderB).
		WithFlag(pcmd.FlagReclamationWindow, storageReclamationWindow.String()).
		WithFlag(pcmd.FlagLeaseFundsMonitorInterval, "1m").
		WithFlag(pcmd.FlagWithdrawalPeriod, "1m")

	s.group.Go(func() error {
		_, err := ptestutil.RunLocalProvider(
			s.ctx,
			cctx,
			pArgsB...,
		)
		if err != nil {
			s.T().Logf("provider B stopped with error: %v", err)
		}

		return err
	})

	s.T().Log("waiting for provider B gateway")
	waitForTCPSocket(s.ctx, dialer, provBHost, s.T())
}

// TestVolumeMigration is §8.3: reclaim at provider A -> Exporting freeze
// -> auto-re-order -> provider B wins -> full-copy over VolumeTransfer
// (local-path file driver, mTLS x/cert identities, chain-derived
// authorization) -> re-attach at provider B -> UUID intact on the
// migrated volume.
func (s *E2EStorageMarketMigration) TestVolumeMigration() {
	const (
		vid          = "e2e-migrate"
		volDSeq      = uint64(300)
		computeDSeq  = uint64(301)
		compute2DSeq = uint64(302)
		host1        = "migrate.localhost"
		host2        = "migrated.localhost"
	)

	tenant := s.addrTenant.String()
	volName := crd.VolumeName(tenant, vid)

	// volume at provider A
	volSDL := s.writeSDL("volume-migrate.yaml", fmt.Sprintf(sdlVolumeTemplate, vid))
	volID := s.createVolume(volSDL, volDSeq, uactMinDeposit)
	volGID := dtypes.MakeGroupID(volID, 1)

	volBidA := mtypes.MakeBidID(mtypes.MakeOrderID(volGID, 1), s.addrProvider)
	s.waitForBid(volBidA)
	volLeaseA := s.createLease(volBidA)

	s.waitForVolume(storageManifestNS, volName, 2*time.Minute, "phase Provisioned at A", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.PVName != ""
	})

	pvPathA := s.volumePVHostPath(storageManifestNS, volName)
	if _, err := os.Stat(pvPathA); err != nil {
		s.Require().FailNowf("local-path PV not visible on the host",
			"path %s: %v - recreate the cluster with `make kube-cluster-setup-e2e` so the identical-path storage mount from _run/kube/kind-config.yaml is active", pvPathA, err)
	}

	// attach at A and write the UUID through the app
	attachSDL := s.writeSDL("attach-migrate.yaml", fmt.Sprintf(sdlComputeAttachTemplate, vid, tenant, volDSeq, host1))
	computeID := s.createComputeDeployment(attachSDL, computeDSeq)

	computeBidA := mtypes.MakeBidID(mtypes.MakeOrderID(dtypes.MakeGroupID(computeID, 1), 1), s.addrProvider)
	s.waitForBid(computeBidA)
	computeLeaseA := s.createLease(computeBidA)

	_, err := ptestutil.ExecSendManifest(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(attachSDL).
			WithHome(s.validator.ClientCtx.HomeDir).
			WithFrom(tenant).
			WithDSeq(computeLeaseA.DSeq).
			WithOutputJSON()...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))

	testData := uuid.New().String()
	s.appWrite(host1, "value", testData)
	s.Require().Equal(testData, s.appRead(host1, "value"))

	// stage the UUID as the volume's image - the file driver's unit of
	// transfer for local-path PVs (an RBD volume exports its real block
	// image instead; that leg is validated on a Rook pair out-of-band)
	s.Require().NoError(os.WriteFile(filepath.Join(pvPathA, "image"), []byte(testData), 0o644))

	// detach at A before reclaiming
	s.closeComputeDeployment(computeDSeq)
	s.waitForVolume(storageManifestNS, volName, 3*time.Minute, "re-parked at A", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.AttachedLease == ""
	})

	// reclaim: the tenant close of the volume LEASE routes through the
	// reclamation window as a migration; A's CRD freezes into Exporting
	closeVolLease := func() {
		res, err := clitestutil.ExecCloseLease(
			s.ctx,
			s.cctx,
			cli.TestFlags().
				WithLeaseID(volLeaseA).
				WithFrom(tenant).
				Append(cliFlags)...,
		)
		s.Require().NoError(err)
		s.Require().NoError(s.waitForBlocksCommitted(1))
		clitestutil.ValidateTxSuccessful(s.ctx, s.T(), s.cctx, res.Bytes())
	}

	closeVolLease()

	s.waitForVolume(storageManifestNS, volName, 2*time.Minute, "phase Exporting at A", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseExporting
	})

	time.Sleep(storageReclamationWindow + storageReclamationFloor)
	closeVolLease()

	// auto-re-order: the same group re-lists; provider B wins the new
	// order
	volBidB := mtypes.MakeBidID(mtypes.MakeOrderID(volGID, 2), s.addrProviderB)
	s.waitForBid(volBidB)
	s.createLease(volBidB)

	// B provisions a fresh Retain PV in its own namespace
	s.waitForVolume(storageManifestNSB, volName, 2*time.Minute, "phase Provisioned at B", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.PVName != ""
	})
	pvPathB := s.volumePVHostPath(storageManifestNSB, volName)

	// pull the full copy over A's VolumeTransfer gateway: B's x/cert
	// identity as the client certificate, A's identity pinned, both
	// authorizations derived from chain state
	kpm, err := atls.NewKeyPairManager(s.cctx, s.addrProviderB)
	s.Require().NoError(err)

	_, certB, err := kpm.ReadX509KeyPair()
	s.Require().NoError(err)

	qc, err := aclient.DiscoverQueryClient(s.ctx, s.cctx)
	s.Require().NoError(err)

	transferClient, closer, err := replication.Dial(
		s.ctx,
		s.grpcHostProvider,
		s.addrProvider.String(),
		[]tls.Certificate{certB},
		pclient.NewCertificateQuerier(qc),
	)
	s.Require().NoError(err)
	defer func() { _ = closer.Close() }()

	syncer := &replication.Syncer{
		Client:    transferClient,
		Driver:    replication.NewFileDriver(""),
		Ref:       replication.Ref{Name: volName, Path: pvPathB},
		Owner:     tenant,
		Vid:       vid,
		DSeq:      volDSeq,
		Requester: s.addrProviderB.String(),
		SpoolDir:  s.network.BaseDir,
	}

	applied, err := syncer.SyncOnce(s.ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(applied, "the first sync must land a full export")

	imported, err := os.ReadFile(filepath.Join(pvPathB, "image"))
	s.Require().NoError(err)
	s.Require().Equal(testData, string(imported), "the UUID must arrive intact at provider B's PV")

	// re-attach at B: colocation now points at the volume's new provider
	attach2SDL := s.writeSDL("attach-migrated.yaml", fmt.Sprintf(sdlComputeAttachTemplate, vid, tenant, volDSeq, host2))
	compute2ID := s.createComputeDeployment(attach2SDL, compute2DSeq)

	compute2BidB := mtypes.MakeBidID(mtypes.MakeOrderID(dtypes.MakeGroupID(compute2ID, 1), 1), s.addrProviderB)
	s.waitForBid(compute2BidB)
	compute2Lease := s.createLease(compute2BidB)

	_, err = ptestutil.ExecSendManifest(
		s.ctx,
		s.cctx,
		cli.TestFlags().
			With(attach2SDL).
			WithHome(s.validator.ClientCtx.HomeDir).
			WithFrom(tenant).
			WithDSeq(compute2Lease.DSeq).
			WithOutputJSON()...,
	)
	s.Require().NoError(err)
	s.Require().NoError(s.waitForBlocksCommitted(2))

	s.waitForVolume(storageManifestNSB, volName, 3*time.Minute, "phase Attached at B", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseAttached
	})

	// the app serves from the migrated volume at B
	httpResp := queryAppWithRetries(s.T(), fmt.Sprintf("http://%s:%s/GET/value", s.appHost, s.appPort), host2, 120)
	s.Require().Equal(http.StatusOK, httpResp.StatusCode)

	// and the UUID is intact on the volume B now serves
	migrated, err := os.ReadFile(filepath.Join(pvPathB, "image"))
	s.Require().NoError(err)
	s.Require().Equal(testData, string(migrated), "the UUID must survive migration and re-attachment at provider B")

	// orderly close: compute at B, then the volume (now B's lease)
	// through its window
	s.closeComputeDeployment(compute2DSeq)
	s.waitForVolume(storageManifestNSB, volName, 3*time.Minute, "re-parked at B", func(v *crd.Volume) bool {
		return v.Status.Phase == crd.VolumePhaseProvisioned && v.Status.AttachedLease == ""
	})
	s.closeVolumeDeployment(volDSeq)
}
