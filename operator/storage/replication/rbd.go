package replication

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// rbdToolsApp is the pod label/container the rbd commands run in - the
	// same rook-ceph-tools deployment the inventory operator's ceph
	// querier execs into.
	rbdToolsApp = "rook-ceph-tools"

	// rbdSnapPrefix namespaces storage-market snapshots so the driver never
	// touches snapshots created by CSI or an operator for other purposes.
	rbdSnapPrefix = "akash-xfer-"

	// rbdOutputLimit bounds what the driver reads back from a command that
	// is not a stream: an image's snapshot list, its info, a digest.
	rbdOutputLimit = 4 << 20

	// rbdStderrLimit bounds the error output kept from a command.
	rbdStderrLimit = 16 << 10
)

// rbdSnapName matches the snapshots the driver takes. Snapshot names also
// arrive from other providers - the diff base a destination asks for, the
// snapshot a source says it serves - and are handed to rbd, so the driver
// takes no other.
var rbdSnapName = regexp.MustCompile(`^akash-xfer-[0-9]{8}T[0-9]{6}Z$`)

// PodLocator resolves the name of the pod carrying the app label.
type PodLocator func(ctx context.Context, namespace, appLabel string) (string, error)

// RBDDriver is the stream-export backend against a Rook/Ceph cluster:
// point-in-time snapshots with `rbd snap create`, full images with
// `rbd export`, incrementals with `rbd export-diff`, all run in the
// rook-ceph-tools pod. No Ceph cluster peering, no WAN-exposed mons, no
// pool-scoped cephx credentials leave the cluster (IMPLEMENTATION.md §5.5
// driver 1).
//
// Images stream: an export is read from rbd's stdout as rbd writes it, and
// an import is written to its stdin as it is read, through the exec
// channel, so the provider holds no more of an image than the transfer's
// chunk in flight, whatever the image's size. Commands get their arguments
// directly, never through a shell, except the digest, which pipes the
// export into sha256sum inside the pod and takes the image as a positional
// argument.
//
// Snapshots are taken directly with `rbd snap create` against the image
// resolved from the PV's CSI volumeAttributes.
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

// rbdError is a command that failed, with what it said on stderr.
type rbdError struct {
	cmd    string
	err    error
	stderr string
}

func (e *rbdError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.cmd, e.err.Error(), e.stderr)
}

func (e *rbdError) Unwrap() []error {
	return []error{ErrReplication, e.err}
}

// notFound reports a command that failed because the image or snapshot it
// names does not exist.
func notFound(err error) bool {
	var rerr *rbdError

	return errors.As(err, &rerr) && strings.Contains(rerr.stderr, "No such file or directory")
}

// command runs cmd in the tools pod, stdin streamed into it and its stdout
// streamed into stdout.
func (d *RBDDriver) command(ctx context.Context, stdin io.Reader, stdout io.Writer, cmd ...string) error {
	pod, err := d.locate(ctx, d.ns, rbdToolsApp)
	if err != nil {
		return fmt.Errorf("%w: locating %s pod: %s", ErrReplication, rbdToolsApp, err.Error())
	}

	stderr := &tailBuffer{limit: rbdStderrLimit}

	err = d.exec.Exec(ctx, PodCommand{
		Namespace: d.ns,
		Pod:       pod,
		Container: rbdToolsApp,
		Command:   cmd,
		Stdin:     stdin,
		Stdout:    stdout,
		Stderr:    stderr,
	})
	if err != nil {
		return &rbdError{cmd: strings.Join(cmd, " "), err: err, stderr: strings.TrimSpace(stderr.String())}
	}

	return nil
}

// run runs cmd and returns its output, which is small.
func (d *RBDDriver) run(ctx context.Context, cmd ...string) (string, error) {
	stdout := &limitedBuffer{limit: rbdOutputLimit}

	if err := d.command(ctx, nil, stdout, cmd...); err != nil {
		return "", err
	}

	return stdout.String(), nil
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
		if !rbdSnapName.MatchString(e.Name) {
			continue
		}

		created, _ := time.Parse(time.ANSIC, e.Timestamp)

		snaps = append(snaps, Snapshot{ID: e.Name, CreatedAt: created})
	}

	// timestamp-encoded ids sort oldest-first lexicographically
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].ID < snaps[j].ID })

	return snaps, nil
}

// holds reports whether the image holds every one of the snapshots named.
func (d *RBDDriver) holds(ctx context.Context, ref Ref, ids ...string) (bool, error) {
	snaps, err := d.Snapshots(ctx, ref)
	if err != nil {
		return false, err
	}

	held := make(map[string]bool, len(snaps))
	for _, snap := range snaps {
		held[snap.ID] = true
	}

	for _, id := range ids {
		if id != "" && !held[id] {
			return false, nil
		}
	}

	return true, nil
}

// Export streams the image at snapshot, or the diff from fromSnapshot to
// it, as rbd writes it. Closing the stream ends the export.
func (d *RBDDriver) Export(ctx context.Context, ref Ref, snapshot, fromSnapshot string) (io.ReadCloser, error) {
	img, err := d.image(ref)
	if err != nil {
		return nil, err
	}

	if !rbdSnapName.MatchString(snapshot) || (fromSnapshot != "" && !rbdSnapName.MatchString(fromSnapshot)) {
		return nil, fmt.Errorf("%w: %q from %q", ErrUnknownSnapshot, snapshot, fromSnapshot)
	}

	// a snapshot missing would fail the export only once it streams
	held, err := d.holds(ctx, ref, snapshot, fromSnapshot)
	if err != nil {
		return nil, err
	}

	if !held {
		return nil, fmt.Errorf("%w: %q from %q", ErrUnknownSnapshot, snapshot, fromSnapshot)
	}

	cmd := []string{"rbd", "export", "--no-progress", img + "@" + snapshot, "-"}
	if fromSnapshot != "" {
		cmd = []string{"rbd", "export-diff", "--no-progress", "--from-snap", fromSnapshot, img + "@" + snapshot, "-"}
	}

	return d.stream(ctx, cmd), nil
}

// stream runs cmd with its stdout piped to the returned reader.
func (d *RBDDriver) stream(ctx context.Context, cmd []string) io.ReadCloser {
	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	done := make(chan struct{})

	go func() {
		defer close(done)

		// a failure reaches the reader once it has read what came before
		_ = pw.CloseWithError(d.command(ctx, nil, pw, cmd...))
	}()

	return &commandStream{PipeReader: pr, cancel: cancel, done: done}
}

// commandStream is a command's stdout; closing it ends the command.
type commandStream struct {
	*io.PipeReader
	cancel context.CancelFunc
	done   <-chan struct{}
}

func (s *commandStream) Close() error {
	s.cancel()
	err := s.PipeReader.Close()
	<-s.done

	return err
}

// Import lands a stream produced by Export. A full image replaces the
// image outright - the one CSI provisioned, or what an earlier import left,
// snapshots and all; the volume is parked or importing, so nothing has it
// mapped - and the snapshot it was taken at is recreated, as the next
// diff's base. A diff lands on the image, and rbd records the snapshot it
// ends at itself.
func (d *RBDDriver) Import(ctx context.Context, ref Ref, snapshot, fromSnapshot string, r io.Reader) error {
	img, err := d.image(ref)
	if err != nil {
		return err
	}

	if !rbdSnapName.MatchString(snapshot) || (fromSnapshot != "" && !rbdSnapName.MatchString(fromSnapshot)) {
		return fmt.Errorf("%w: snapshot %q from %q is none this driver takes", ErrReplication, snapshot, fromSnapshot)
	}

	if fromSnapshot != "" {
		return d.command(ctx, r, nil, "rbd", "import-diff", "--no-progress", "-", img)
	}

	if err := d.command(ctx, nil, nil, "rbd", "snap", "purge", "--no-progress", img); err != nil && !notFound(err) {
		return err
	}

	if err := d.command(ctx, nil, nil, "rbd", "rm", "--no-progress", img); err != nil && !notFound(err) {
		return err
	}

	if err := d.command(ctx, r, nil, "rbd", "import", "--no-progress", "--export-format", "1", "-", img); err != nil {
		return err
	}

	_, err = d.run(ctx, "rbd", "snap", "create", img+"@"+snapshot)

	return err
}

// rbdDigestScript hashes an image inside the pod, so the image never
// crosses the exec channel; the image comes in as $1. pipefail fails the
// digest when the export fails, rather than hashing what came out.
const rbdDigestScript = `set -o pipefail; rbd export --no-progress "$1" - | sha256sum`

func (d *RBDDriver) Digest(ctx context.Context, ref Ref, snapshot string) (Digest, error) {
	img, err := d.image(ref)
	if err != nil {
		return Digest{}, err
	}

	if !rbdSnapName.MatchString(snapshot) {
		return Digest{}, fmt.Errorf("%w: %q", ErrUnknownSnapshot, snapshot)
	}

	info, err := d.run(ctx, "rbd", "info", img+"@"+snapshot, "--format", "json")
	if err != nil {
		if notFound(err) {
			return Digest{}, fmt.Errorf("%w: %q: %s", ErrUnknownSnapshot, snapshot, err.Error())
		}

		return Digest{}, err
	}

	var parsed struct {
		Size uint64 `json:"size"`
	}
	if err := json.Unmarshal([]byte(info), &parsed); err != nil {
		return Digest{}, fmt.Errorf("%w: parsing rbd info: %s", ErrReplication, err.Error())
	}

	sumOut, err := d.run(ctx, "sh", "-c", rbdDigestScript, "sh", img+"@"+snapshot)
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

// limitedBuffer holds a command's output up to limit bytes and fails the
// write that would pass it.
type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, fmt.Errorf("%w: command output passes %d bytes", ErrReplication, b.limit)
	}

	return b.Buffer.Write(p)
}

// tailBuffer keeps the last limit bytes written to it. A cancelled exec
// returns before its streams wind down, so writes may still land while
// the error is read.
type tailBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buf = append(b.buf, p...)

	if over := len(b.buf) - b.limit; over > 0 {
		b.buf = append(b.buf[:0], b.buf[over:]...)
	}

	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.buf)
}
