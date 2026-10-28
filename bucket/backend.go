// Package bucket serves storage-market bucket volumes: S3-compatible buckets the
// provider exposes over the network to holders of the owner's access keys.
// A Backend maps one bucket onto an S3 server; RGW is the first.
package bucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"regexp"
	"strings"
)

// KeyAccess is what an access key may do. Neither level can change the
// bucket's ACLs or policy, so a key holder cannot make the bucket public.
type KeyAccess string

const (
	KeyReadWrite KeyAccess = "readwrite"
	KeyRead      KeyAccess = "read"
)

// MaxKeys bounds the access keys of one bucket.
const MaxKeys = 20

var (
	ErrNotFound = errors.New("bucket not found")
	// ErrConflict is a bucket or backend user that belongs to another volume.
	ErrConflict       = errors.New("bucket belongs to another volume")
	ErrKeyExists      = errors.New("an access key with this name exists")
	ErrKeyNotFound    = errors.New("access key not found")
	ErrTooManyKeys    = errors.New("bucket access key limit reached")
	ErrInvalidKeyName = errors.New("invalid access key name")
	ErrInvalidAccess  = errors.New("invalid access key access level")
	ErrObjectNotFound = errors.New("object not found")
)

// Ref names a bucket, the backend user that owns it and the volume both
// belong to. Backends check the volume on every call: UserID is a truncated
// hash that another volume can be made to share.
type Ref struct {
	// UserID is the backend user that owns the bucket and whose sub-users
	// hold the tenant's keys. It is stable across leases (the Volume CRD
	// name), so an adoption keeps the data.
	UserID string
	Bucket string
	Owner  string
	Vid    string
}

// Spec is what Ensure provisions.
type Spec struct {
	Ref
	SizeBytes int64
}

// KeyInfo describes an access key without its secret.
type KeyInfo struct {
	Name        string
	Access      KeyAccess
	AccessKeyID string
}

// Key is a new access key with the secret, returned once.
type Key struct {
	KeyInfo
	SecretAccessKey string
}

// Usage is what a bucket holds.
type Usage struct {
	UsedBytes int64
	Objects   int64
}

// ObjectInfo describes an object: its key, its size and the headers it is
// served with. A listing fills only Key and Size.
type ObjectInfo struct {
	Key                string
	Size               int64
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ContentLanguage    string
	CacheControl       string
	Expires            string
	// Metadata is the object's user metadata, keyed by its lower-case name
	// without the x-amz-meta- prefix.
	Metadata map[string]string
}

// Objects reaches one bucket's objects as the provider itself, to move them
// between providers. It signs with a key of the bucket's owner that Close
// drops, so the provider keeps no key to the tenant's data.
type Objects interface {
	// List returns up to limit objects whose keys sort after after, in key
	// order, and whether more follow them.
	List(ctx context.Context, after string, limit int) ([]ObjectInfo, bool, error)
	// Get opens an object. The caller closes its body.
	Get(ctx context.Context, key string) (ObjectInfo, io.ReadCloser, error)
	// Put writes an object whole. body holds exactly info.Size bytes whose
	// sha256 is sum, which the S3 server checks them against.
	Put(ctx context.Context, info ObjectInfo, body io.ReaderAt, sum []byte) error
	// Delete removes an object. A missing one is not an error.
	Delete(ctx context.Context, key string) error
	Close() error
}

// Backend provisions buckets and their access keys on an S3 server.
type Backend interface {
	// Ensure creates the bucket's user, quota and bucket, or brings an
	// existing one to the spec. It is idempotent.
	Ensure(ctx context.Context, spec Spec) error
	// Suspend stops every key of the bucket at once.
	Suspend(ctx context.Context, ref Ref) error
	// Resume lifts a suspension and revokes every key, so the bucket's new
	// lease starts with none.
	Resume(ctx context.Context, ref Ref) error
	// Seal revokes every key of the bucket, so nothing writes to it again
	// and only the provider still reads it, to hand its objects to the
	// provider the bucket moves to.
	Seal(ctx context.Context, ref Ref) error
	// Objects opens the bucket's objects to the provider itself. It fails
	// with ErrNotFound when the bucket does not exist.
	Objects(ctx context.Context, ref Ref) (Objects, error)
	// Delete removes the bucket, its objects and its user. It is idempotent.
	Delete(ctx context.Context, ref Ref) error
	CreateKey(ctx context.Context, ref Ref, name string, access KeyAccess) (Key, error)
	ListKeys(ctx context.Context, ref Ref) ([]KeyInfo, error)
	RevokeKey(ctx context.Context, ref Ref, name string) error
	Usage(ctx context.Context, ref Ref) (Usage, error)
}

// keyName is a DNS label, like a vid.
var keyName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,30}[a-z0-9])?$`)

// ValidateKeyName checks a tenant-chosen access key name.
func ValidateKeyName(name string) error {
	if !keyName.MatchString(name) {
		return ErrInvalidKeyName
	}

	return nil
}

// ValidateKeyAccess checks a requested access level.
func ValidateKeyAccess(access KeyAccess) error {
	switch access {
	case KeyReadWrite, KeyRead:
		return nil
	default:
		return ErrInvalidAccess
	}
}

// bucketHashLen hex digits (64 bits) of the volume's hash end a bucket
// name: too many to make two volumes share one by search.
const bucketHashLen = 16

// RefFor names the bucket of a volume. userID is the Volume CRD name; the
// bucket is the vid followed by a hash of owner/vid, so two owners can use
// the same vid on one provider and a shortened vid stays unique.
func RefFor(userID, owner, vid string) Ref {
	sum := sha256.Sum256([]byte(owner + "/" + vid))
	suffix := hex.EncodeToString(sum[:])[:bucketHashLen]

	// S3 bucket names are at most 63 characters and end in a letter or digit.
	prefix := vid
	if limit := 63 - 1 - bucketHashLen; len(prefix) > limit {
		prefix = strings.TrimRight(prefix[:limit], "-")
	}

	return Ref{UserID: userID, Bucket: prefix + "-" + suffix, Owner: owner, Vid: vid}
}

// displayName records on the backend user which volume it belongs to.
func displayName(ref Ref) string {
	return ref.Owner + "/" + ref.Vid
}
