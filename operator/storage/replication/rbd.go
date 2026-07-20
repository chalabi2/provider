package replication

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	rookexec "github.com/rook/rook/pkg/util/exec"
)

const (
	// rbdToolsApp is the pod label/container the rbd commands run in - the
	// same rook-ceph-tools deployment the inventory operator's ceph
	// querier execs into.
	rbdToolsApp = "rook-ceph-tools"

	// rbdSnapPrefix namespaces AEP-87 snapshots so the driver never
	// touches snapshots created by CSI or an operator for other purposes.
	rbdSnapPrefix = "akash-xfer-"
)

// PodExec is the narrow slice of the inventory operator's
// RemotePodCommandExecutor the RBD backend needs (rook exec against the
// rook-ceph-tools pod).
type PodExec interface {
	ExecWithOptions(ctx context.Context, options rookexec.ExecOptions) (string, string, error)
	ExecCommandInContainerWithFullOutput(ctx context.Context, appLabel, containerName, namespace string, cmd ...string) (string, string, error)
}

// PodLocator resolves the rook-ceph-tools pod name for stdin execs
// (ExecCommandInContainerWithFullOutput resolves the pod itself but takes
// no stdin).
type PodLocator func(ctx context.Context, namespace, appLabel string) (string, error)

// RBDDriver is the stream-export backend against a Rook/Ceph cluster:
// point-in-time snapshots with `rbd snap create`, full images with
// `rbd export`, incrementals with `rbd export-diff`, all executed in the
// rook-ceph-tools pod via the existing RemotePodCommandExecutor. No Ceph
// cluster peering, no WAN-exposed mons, no pool-scoped cephx credentials
// leave the cluster (IMPLEMENTATION.md §5.5 driver 1).
//
// Snapshots are taken directly with `rbd snap create` against the image
// resolved from the PV's CSI volumeAttributes; orchestrating them through
// CSI VolumeSnapshot objects is an additive follow-up (the RBAC for
// snapshot.storage.k8s.io is already granted to the storage operator).
//
// LIMITATION: kube pod exec buffers command output, so export payloads
// transit base64-encoded through the exec channel and are held in memory
// on both sides. That bounds practical image size and is acceptable for
// the plumbing this stage ships; CI exercises the FileDriver, and RBD
// correctness at size is validated on a real two-cluster Rook pair via
// the manual runbook (IMPLEMENTATION.md §8.3), where a streaming exec
// (SPDY writer) replaces the buffered path.
type RBDDriver struct {
	exec   PodExec
	locate PodLocator
	ns     string
	now    func() time.Time
}

var _ Driver = (*RBDDriver)(nil)

// NewRBDDriver returns the rbd stream-export backend execing into the
// rook-ceph-tools pod in namespace ns.
func NewRBDDriver(exec PodExec, locate PodLocator, ns string) *RBDDriver {
	return &RBDDriver{
		exec:   exec,
		locate: locate,
		ns:     ns,
		now:    time.Now,
	}
}

func (d *RBDDriver) image(ref Ref) (string, error) {
	if ref.Pool == "" || ref.Image == "" {
		return "", fmt.Errorf("%w: volume %s has no rbd pool/image", ErrReplication, ref.Name)
	}

	return ref.Pool + "/" + ref.Image, nil
}

func (d *RBDDriver) run(ctx context.Context, cmd ...string) (string, error) {
	stdout, stderr, err := d.exec.ExecCommandInContainerWithFullOutput(ctx, rbdToolsApp, rbdToolsApp, d.ns, cmd...)
	if err != nil {
		return "", fmt.Errorf("%w: %s: %s: %s", ErrReplication, strings.Join(cmd, " "), err.Error(), stderr)
	}

	return stdout, nil
}

func (d *RBDDriver) Snapshot(ctx context.Context, ref Ref) (Snapshot, error) {
	img, err := d.image(ref)
	if err != nil {
		return Snapshot{}, err
	}

	now := d.now().UTC()
	id := rbdSnapPrefix + now.Format("20060102T150405Z")

	if _, err := d.run(ctx, "rbd", "snap", "create", img+"@"+id); err != nil {
		return Snapshot{}, err
	}

	return Snapshot{ID: id, CreatedAt: now}, nil
}

// rbdSnapEntry is the subset of `rbd snap ls --format json` the driver
// reads.
type rbdSnapEntry struct {
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
}

func (d *RBDDriver) Snapshots(ctx context.Context, ref Ref) ([]Snapshot, error) {
	img, err := d.image(ref)
	if err != nil {
		return nil, err
	}

	stdout, err := d.run(ctx, "rbd", "snap", "ls", img, "--format", "json")
	if err != nil {
		return nil, err
	}

	var entries []rbdSnapEntry
	if err := json.Unmarshal([]byte(stdout), &entries); err != nil {
		return nil, fmt.Errorf("%w: parsing rbd snap ls: %s", ErrReplication, err.Error())
	}

	snaps := make([]Snapshot, 0, len(entries))

	for _, e := range entries {
		if !strings.HasPrefix(e.Name, rbdSnapPrefix) {
			continue
		}

		created, _ := time.Parse(time.ANSIC, e.Timestamp)

		snaps = append(snaps, Snapshot{ID: e.Name, CreatedAt: created})
	}

	// timestamp-encoded ids sort oldest-first lexicographically
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].ID < snaps[j].ID })

	return snaps, nil
}

func (d *RBDDriver) Export(ctx context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error) {
	img, err := d.image(ref)
	if err != nil {
		return nil, err
	}

	var rbdCmd string
	if fromSnapshot == "" {
		rbdCmd = fmt.Sprintf("rbd export %s@%s -", img, snapshot)
	} else {
		rbdCmd = fmt.Sprintf("rbd export-diff --from-snap %s %s@%s -", fromSnapshot, img, snapshot)
	}

	stdout, err := d.run(ctx, "sh", "-c", rbdCmd+" | base64 -w0")
	if err != nil {
		if strings.Contains(err.Error(), "No such file or directory") || strings.Contains(err.Error(), "not found") {
			return nil, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
		}

		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(stdout))
	if err != nil {
		return nil, fmt.Errorf("%w: decoding export payload: %s", ErrReplication, err.Error())
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (d *RBDDriver) Import(ctx context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	img, err := d.image(ref)
	if err != nil {
		return err
	}

	pod, err := d.locate(ctx, d.ns, rbdToolsApp)
	if err != nil {
		return fmt.Errorf("%w: locating %s pod: %s", ErrReplication, rbdToolsApp, err.Error())
	}

	var rbdCmd string
	if fromSnapshot == "" {
		// a full import lands on a fresh image name; the CSI-provisioned
		// image is replaced by removing it first (the destination PV is
		// parked, nothing has it mapped)
		rbdCmd = fmt.Sprintf("rbd rm %s 2>/dev/null; base64 -d | rbd import --export-format 1 - %s", img, img)
	} else {
		rbdCmd = fmt.Sprintf("base64 -d | rbd import-diff - %s", img)
	}

	payload := &bytes.Buffer{}

	enc := base64.NewEncoder(base64.StdEncoding, payload)
	if _, err := io.Copy(enc, r); err != nil {
		return err
	}

	if err := enc.Close(); err != nil {
		return err
	}

	_, stderr, err := d.exec.ExecWithOptions(ctx, rookexec.ExecOptions{
		Command:            []string{"sh", "-c", rbdCmd},
		Namespace:          d.ns,
		PodName:            pod,
		ContainerName:      rbdToolsApp,
		Stdin:              payload,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: false,
	})
	if err != nil {
		return fmt.Errorf("%w: rbd import: %s: %s", ErrReplication, err.Error(), stderr)
	}

	// a full import carries no snapshots; recreate the diff base so the
	// next incremental has its from-snap
	if _, err := d.run(ctx, "rbd", "snap", "create", img+"@"+snapshot); err != nil {
		return err
	}

	return nil
}

func (d *RBDDriver) Digest(ctx context.Context, ref Ref, snapshot string) (Digest, error) {
	img, err := d.image(ref)
	if err != nil {
		return Digest{}, err
	}

	info, err := d.run(ctx, "rbd", "info", img+"@"+snapshot, "--format", "json")
	if err != nil {
		return Digest{}, err
	}

	var parsed struct {
		Size uint64 `json:"size"`
	}
	if err := json.Unmarshal([]byte(info), &parsed); err != nil {
		return Digest{}, fmt.Errorf("%w: parsing rbd info: %s", ErrReplication, err.Error())
	}

	sumOut, err := d.run(ctx, "sh", "-c", fmt.Sprintf("rbd export %s@%s - | sha256sum", img, snapshot))
	if err != nil {
		return Digest{}, err
	}

	fields := strings.Fields(sumOut)
	if len(fields) == 0 {
		return Digest{}, fmt.Errorf("%w: empty sha256sum output", ErrReplication)
	}

	sum, err := hex.DecodeString(fields[0])
	if err != nil {
		return Digest{}, fmt.Errorf("%w: parsing sha256sum: %s", ErrReplication, err.Error())
	}

	return Digest{Size: parsed.Size, SHA256: sum}, nil
}

func (d *RBDDriver) Prune(ctx context.Context, ref Ref, keep int) error {
	img, err := d.image(ref)
	if err != nil {
		return err
	}

	if keep < 1 {
		keep = 1
	}

	snaps, err := d.Snapshots(ctx, ref)
	if err != nil {
		return err
	}

	for len(snaps) > keep {
		if _, err := d.run(ctx, "rbd", "snap", "rm", img+"@"+snaps[0].ID); err != nil {
			return err
		}

		snaps = snaps[1:]
	}

	return nil
}
