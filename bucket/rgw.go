package bucket

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // Content-MD5 only
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/ceph/go-ceph/rgw/admin"
)

// rgw serves buckets from Ceph RGW through its Admin Ops API. Each bucket
// has its own RGW user (max one bucket, quota = the leased size) whose
// display name records the volume; tenant keys are that user's sub-users
// with read-write or read access, which RGW does not let change the
// bucket's ACLs or policy.
type rgw struct {
	api      *admin.API
	endpoint string
	region   string
	http     *http.Client
}

const (
	rgwKeyTypeS3 = "s3"
	// RGW refuses to create buckets for a user whose maximum is negative.
	rgwBucketCreationDisabled = -1
)

func newRGW(c Config) (Backend, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	endpoint := strings.TrimRight(c.AdminEndpoint, "/")

	api, err := admin.New(endpoint, c.AdminAccessKey, c.AdminSecretKey, client)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	return &rgw{api: api, endpoint: endpoint, region: c.Region, http: client}, nil
}

func (b *rgw) Ensure(ctx context.Context, spec Spec) error {
	exists, err := b.bucketOwnedBy(ctx, spec.Ref)
	if err != nil {
		return err
	}

	user, err := b.user(ctx, spec.Ref)
	if errors.Is(err, ErrNotFound) {
		user, err = b.api.CreateUser(ctx, admin.User{ID: spec.UserID, DisplayName: displayName(spec.Ref)})
		if err != nil {
			err = fmt.Errorf("create bucket user %s: %w", spec.UserID, err)
		}
	}
	if err != nil {
		return err
	}

	enabled := true
	size := spec.SizeBytes
	if err := b.api.SetUserQuota(ctx, admin.QuotaSpec{UID: spec.UserID, Enabled: &enabled, MaxSize: &size}); err != nil {
		return fmt.Errorf("set quota of %s: %w", spec.UserID, err)
	}

	err = b.asOwner(ctx, user, func(owner s3Signer) error {
		if !exists {
			if err := b.setMaxBuckets(ctx, spec.UserID, 1); err != nil {
				return err
			}
			if err := b.createBucket(ctx, owner, spec.Bucket); err != nil {
				return err
			}
		}

		return b.guardBucket(ctx, owner, spec.Bucket)
	})
	if err != nil {
		return err
	}

	// A user that may create buckets could, after emptying and deleting its
	// own, take a name another volume's bucket will need.
	return b.setMaxBuckets(ctx, spec.UserID, rgwBucketCreationDisabled)
}

// bucketOwnedBy reports whether ref's bucket exists, refusing one that
// another user owns: it must never be handed out or destroyed.
func (b *rgw) bucketOwnedBy(ctx context.Context, ref Ref) (bool, error) {
	info, err := b.api.GetBucketInfo(ctx, admin.Bucket{Bucket: ref.Bucket})
	switch {
	case errors.Is(err, admin.ErrNoSuchBucket):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read bucket %s: %w", ref.Bucket, err)
	case info.Owner != ref.UserID:
		return false, fmt.Errorf("%w: bucket %s", ErrConflict, ref.Bucket)
	default:
		return true, nil
	}
}

func (b *rgw) setMaxBuckets(ctx context.Context, uid string, n int) error {
	if _, err := b.api.ModifyUser(ctx, admin.User{ID: uid, MaxBuckets: &n}); err != nil {
		return fmt.Errorf("set max buckets of %s to %d: %w", uid, n, err)
	}

	return nil
}

// asOwner runs fn with a key of the user itself, which only the provider
// ever holds and which is dropped when fn returns, so the provider keeps no
// key to the tenant's data.
func (b *rgw) asOwner(ctx context.Context, user admin.User, fn func(s3Signer) error) error {
	key, ok := ownerKey(user)
	if !ok {
		generate := true
		keys, err := b.api.CreateKey(ctx, admin.UserKeySpec{UID: user.ID, KeyType: rgwKeyTypeS3, GenerateKey: &generate})
		if err != nil {
			return fmt.Errorf("owner key of %s: %w", user.ID, err)
		}
		if key, ok = ownerKey(admin.User{ID: user.ID, Keys: *keys}); !ok {
			return fmt.Errorf("owner key of %s was not created", user.ID)
		}
	}

	err := fn(s3Signer{creds: aws.Credentials{AccessKeyID: key.AccessKey, SecretAccessKey: key.SecretKey}, region: b.region})

	return errors.Join(err, b.dropOwnerKeys(ctx, user.ID))
}

func (b *rgw) createBucket(ctx context.Context, owner s3Signer, bucket string) error {
	status, body, err := b.s3(ctx, owner, http.MethodPut, bucket, "", nil)
	switch {
	case err != nil:
		return fmt.Errorf("create bucket %s: %w", bucket, err)
	case status == http.StatusOK:
		return nil
	case status == http.StatusConflict && bytes.Contains(body, []byte("BucketAlreadyOwnedByYou")):
		return nil
	default:
		return fmt.Errorf("create bucket %s: %d: %s", bucket, status, bytes.TrimSpace(body))
	}
}

// guardBucket stops the bucket's keys from deleting it or making any of it
// public. Keys are sub-users, which cannot change the policy or the access
// block.
func (b *rgw) guardBucket(ctx context.Context, owner s3Signer, bucket string) error {
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Sid":"AkashKeepBucket","Effect":"Deny","Principal":"*","Action":["s3:DeleteBucket"],"Resource":["arn:aws:s3:::%s"]}]}`, bucket)

	for _, req := range []struct {
		query string
		body  string
	}{
		{query: "?policy", body: policy},
		{query: "?publicAccessBlock", body: publicAccessBlock},
	} {
		status, body, err := b.s3(ctx, owner, http.MethodPut, bucket, req.query, []byte(req.body))
		if err != nil {
			return fmt.Errorf("guard bucket %s (%s): %w", bucket, req.query, err)
		}
		if status/100 != 2 {
			return fmt.Errorf("guard bucket %s (%s): %d: %s", bucket, req.query, status, bytes.TrimSpace(body))
		}
	}

	return nil
}

// publicAccessBlock makes RGW ignore public ACLs, so an object a key
// uploads as public-read is still refused to anyone without a key.
// BlockPublicAcls stays off: RGW (Squid) refuses every upload under it, not
// only public ones.
const publicAccessBlock = `<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
	`<BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls>` +
	`<BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets>` +
	`</PublicAccessBlockConfiguration>`

type s3Signer struct {
	creds  aws.Credentials
	region string
}

// s3 makes a path-style S3 request to the in-cluster endpoint.
func (b *rgw) s3(ctx context.Context, signer s3Signer, method, bucket, query string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, b.endpoint+"/"+bucket+query, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}

	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if len(body) > 0 {
		md := md5.Sum(body) //nolint:gosec // Content-MD5 is an integrity header S3 asks for, not a security hash
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md[:]))
	}

	if err := v4.NewSigner().SignHTTP(ctx, signer.creds, req, hash, "s3", signer.region, time.Now()); err != nil {
		return 0, nil, err
	}

	resp, err := b.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	reply, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return 0, nil, err
	}

	return resp.StatusCode, reply, nil
}

// ownerKey is a key of the user itself rather than of one of its sub-users.
func ownerKey(user admin.User) (admin.UserKeySpec, bool) {
	for _, key := range user.Keys {
		if key.User == user.ID {
			return key, true
		}
	}

	return admin.UserKeySpec{}, false
}

// dropOwnerKeys leaves the user only its sub-users' keys.
func (b *rgw) dropOwnerKeys(ctx context.Context, uid string) error {
	user, err := b.api.GetUser(ctx, admin.User{ID: uid})
	if err != nil {
		return fmt.Errorf("read bucket user %s: %w", uid, err)
	}

	for _, key := range user.Keys {
		if key.User != uid {
			continue
		}
		if err := b.api.RemoveKey(ctx, admin.UserKeySpec{UID: uid, AccessKey: key.AccessKey, KeyType: rgwKeyTypeS3}); err != nil {
			return fmt.Errorf("drop owner key of %s: %w", uid, err)
		}
	}

	return nil
}

func (b *rgw) Suspend(ctx context.Context, ref Ref) error {
	if _, err := b.user(ctx, ref); err != nil {
		return err
	}

	return b.setSuspended(ctx, ref.UserID, 1)
}

// Resume revokes the keys before lifting the suspension, so no key of the
// previous lease works again even for a moment.
func (b *rgw) Resume(ctx context.Context, ref Ref) error {
	user, err := b.user(ctx, ref)
	if err != nil {
		return err
	}

	for _, sub := range user.Subusers {
		if err := b.removeSubuser(ctx, ref.UserID, subuserName(ref.UserID, sub.Name)); err != nil {
			return err
		}
	}

	return b.setSuspended(ctx, ref.UserID, 0)
}

func (b *rgw) setSuspended(ctx context.Context, uid string, suspended int) error {
	if _, err := b.api.ModifyUser(ctx, admin.User{ID: uid, Suspended: &suspended}); err != nil {
		return fmt.Errorf("set suspended=%d on %s: %w", suspended, uid, err)
	}

	return nil
}

func (b *rgw) Delete(ctx context.Context, ref Ref) error {
	_, err := b.user(ctx, ref)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	exists, err := b.bucketOwnedBy(ctx, ref)
	switch {
	case errors.Is(err, ErrConflict):
		// another user's bucket of this name stays; only ref's user goes
	case err != nil:
		return err
	case exists:
		purge := true
		err := b.api.RemoveBucket(ctx, admin.Bucket{Bucket: ref.Bucket, PurgeObject: &purge})
		if err != nil && !errors.Is(err, admin.ErrNoSuchBucket) {
			return fmt.Errorf("delete bucket %s: %w", ref.Bucket, err)
		}
	}

	purgeData := 1
	err = b.api.RemoveUser(ctx, admin.User{ID: ref.UserID, PurgeData: &purgeData})
	if err != nil && !errors.Is(err, admin.ErrNoSuchUser) {
		return fmt.Errorf("delete bucket user %s: %w", ref.UserID, err)
	}

	return nil
}

func (b *rgw) CreateKey(ctx context.Context, ref Ref, name string, access KeyAccess) (Key, error) {
	if err := ValidateKeyName(name); err != nil {
		return Key{}, err
	}
	if err := ValidateKeyAccess(access); err != nil {
		return Key{}, err
	}

	user, err := b.user(ctx, ref)
	if err != nil {
		return Key{}, err
	}

	if len(user.Subusers) >= MaxKeys {
		return Key{}, ErrTooManyKeys
	}

	// RGW answers a create of an existing sub-user by changing it
	full := ref.UserID + ":" + name
	for _, sub := range user.Subusers {
		if sub.Name == full {
			return Key{}, ErrKeyExists
		}
	}

	// RGW generates the S3 key pair of a sub-user created without one
	generate := true
	keyType := rgwKeyTypeS3
	err = b.api.CreateSubuser(ctx, admin.User{ID: ref.UserID}, admin.SubuserSpec{
		Name:              name,
		Access:            rgwAccess(access),
		KeyType:           &keyType,
		GenerateAccessKey: &generate,
	})
	if errors.Is(err, admin.ErrSubuserExists) {
		return Key{}, ErrKeyExists
	}
	if err != nil {
		return Key{}, fmt.Errorf("create key %s on %s: %w", name, ref.UserID, err)
	}

	user, err = b.user(ctx, ref)
	if err != nil {
		return Key{}, err
	}

	for _, key := range user.Keys {
		if key.User == full {
			return Key{
				KeyInfo:         KeyInfo{Name: name, Access: access, AccessKeyID: key.AccessKey},
				SecretAccessKey: key.SecretKey,
			}, nil
		}
	}

	return Key{}, fmt.Errorf("key %s on %s was created without an S3 key", name, ref.UserID)
}

func (b *rgw) ListKeys(ctx context.Context, ref Ref) ([]KeyInfo, error) {
	user, err := b.user(ctx, ref)
	if err != nil {
		return nil, err
	}

	ids := make(map[string]string, len(user.Keys))
	for _, key := range user.Keys {
		ids[key.User] = key.AccessKey
	}

	keys := make([]KeyInfo, 0, len(user.Subusers))
	for _, sub := range user.Subusers {
		access := KeyRead
		if sub.Access == admin.SubuserAccessReplyReadWrite {
			access = KeyReadWrite
		}

		keys = append(keys, KeyInfo{Name: subuserName(ref.UserID, sub.Name), Access: access, AccessKeyID: ids[sub.Name]})
	}

	return keys, nil
}

func (b *rgw) RevokeKey(ctx context.Context, ref Ref, name string) error {
	if _, err := b.user(ctx, ref); err != nil {
		return err
	}

	if err := ValidateKeyName(name); err != nil {
		return ErrKeyNotFound
	}

	return b.removeSubuser(ctx, ref.UserID, name)
}

func (b *rgw) removeSubuser(ctx context.Context, uid, name string) error {
	purge := true
	err := b.api.RemoveSubuser(ctx, admin.User{ID: uid}, admin.SubuserSpec{Name: name, PurgeKeys: &purge})
	if errors.Is(err, admin.ErrNoSuchSubUser) {
		return ErrKeyNotFound
	}
	if err != nil {
		return fmt.Errorf("revoke key %s on %s: %w", name, uid, err)
	}

	return nil
}

func (b *rgw) Usage(ctx context.Context, ref Ref) (Usage, error) {
	if _, err := b.user(ctx, ref); err != nil {
		return Usage{}, err
	}

	info, err := b.api.GetBucketInfo(ctx, admin.Bucket{Bucket: ref.Bucket})
	switch {
	case errors.Is(err, admin.ErrNoSuchBucket):
		return Usage{}, ErrNotFound
	case err != nil:
		return Usage{}, fmt.Errorf("read usage of %s: %w", ref.Bucket, err)
	case info.Owner != ref.UserID:
		return Usage{}, fmt.Errorf("%w: bucket %s", ErrConflict, ref.Bucket)
	}

	var usage Usage
	if size := info.Usage.RgwMain.SizeActual; size != nil {
		usage.UsedBytes = clampInt64(*size)
	}
	if objects := info.Usage.RgwMain.NumObjects; objects != nil {
		usage.Objects = clampInt64(*objects)
	}

	return usage, nil
}

func clampInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(v)
}

// user reads ref's backend user, refusing one that belongs to another
// volume.
func (b *rgw) user(ctx context.Context, ref Ref) (admin.User, error) {
	user, err := b.api.GetUser(ctx, admin.User{ID: ref.UserID})
	switch {
	case errors.Is(err, admin.ErrNoSuchUser):
		return admin.User{}, ErrNotFound
	case err != nil:
		return admin.User{}, fmt.Errorf("read bucket user %s: %w", ref.UserID, err)
	case user.DisplayName != displayName(ref):
		return admin.User{}, fmt.Errorf("%w: user %s", ErrConflict, ref.UserID)
	default:
		return user, nil
	}
}

func rgwAccess(access KeyAccess) admin.SubuserAccess {
	if access == KeyReadWrite {
		return admin.SubuserAccessReadWrite
	}

	return admin.SubuserAccessRead
}

// subuserName strips the "uid:" RGW prefixes sub-user ids with.
func subuserName(uid, id string) string {
	return strings.TrimPrefix(id, uid+":")
}
