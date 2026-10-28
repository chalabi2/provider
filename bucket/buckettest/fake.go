// Package buckettest has an in-memory bucket.Backend and the contract every
// backend must meet.
package buckettest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/akash-network/provider/bucket"
)

// ErrRefused is how the fake's data plane refuses a request.
var ErrRefused = errors.New("refused")

type fakeUser struct {
	owner     string
	vid       string
	bucket    string
	suspended bool
	quota     int64
	keys      map[string]bucket.Key
	objects   map[string][]byte
}

// Fake is an in-memory bucket.Backend with a data plane that enforces keys,
// access levels, suspension and quota the way the backend contract asks.
type Fake struct {
	mu      sync.Mutex
	users   map[string]*fakeUser
	buckets map[string]string
	errs    map[string]error
}

var (
	_ bucket.Backend = (*Fake)(nil)
	_ DataPlane      = (*Fake)(nil)
)

func NewFake() *Fake {
	return &Fake{users: map[string]*fakeUser{}, buckets: map[string]string{}, errs: map[string]error{}}
}

// Fail makes every later call of the named method return err; nil clears it.
func (f *Fake) Fail(method string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err == nil {
		delete(f.errs, method)
		return
	}
	f.errs[method] = err
}

func (f *Fake) Exists(ref bucket.Ref) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.users[ref.UserID]
	return ok
}

func (f *Fake) Suspended(ref bucket.Ref) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	user, ok := f.users[ref.UserID]
	return ok && user.suspended
}

func (f *Fake) Quota(ref bucket.Ref) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	if user, ok := f.users[ref.UserID]; ok {
		return user.quota
	}
	return 0
}

func (f *Fake) Ensure(_ context.Context, spec bucket.Spec) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs["Ensure"]; err != nil {
		return err
	}

	if owner, ok := f.buckets[spec.Bucket]; ok && owner != spec.UserID {
		return fmt.Errorf("%w: bucket %s", bucket.ErrConflict, spec.Bucket)
	}

	user, err := f.user(spec.Ref)
	if errors.Is(err, bucket.ErrNotFound) {
		user = &fakeUser{owner: spec.Owner, vid: spec.Vid, bucket: spec.Bucket, keys: map[string]bucket.Key{}, objects: map[string][]byte{}}
		f.users[spec.UserID] = user
		err = nil
	}
	if err != nil {
		return err
	}

	user.quota = spec.SizeBytes
	f.buckets[spec.Bucket] = spec.UserID

	return nil
}

func (f *Fake) Suspend(_ context.Context, ref bucket.Ref) error {
	return f.with("Suspend", ref, func(user *fakeUser) error {
		user.suspended = true
		return nil
	})
}

func (f *Fake) Resume(_ context.Context, ref bucket.Ref) error {
	return f.with("Resume", ref, func(user *fakeUser) error {
		user.keys = map[string]bucket.Key{}
		user.suspended = false
		return nil
	})
}

func (f *Fake) Delete(_ context.Context, ref bucket.Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs["Delete"]; err != nil {
		return err
	}

	user, err := f.user(ref)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if f.buckets[user.bucket] == ref.UserID {
		delete(f.buckets, user.bucket)
	}
	delete(f.users, ref.UserID)

	return nil
}

func (f *Fake) CreateKey(_ context.Context, ref bucket.Ref, name string, access bucket.KeyAccess) (bucket.Key, error) {
	if err := bucket.ValidateKeyName(name); err != nil {
		return bucket.Key{}, err
	}
	if err := bucket.ValidateKeyAccess(access); err != nil {
		return bucket.Key{}, err
	}

	var key bucket.Key
	err := f.with("CreateKey", ref, func(user *fakeUser) error {
		if len(user.keys) >= bucket.MaxKeys {
			return bucket.ErrTooManyKeys
		}
		if _, ok := user.keys[name]; ok {
			return bucket.ErrKeyExists
		}

		key = bucket.Key{
			KeyInfo:         bucket.KeyInfo{Name: name, Access: access, AccessKeyID: randomHex(10)},
			SecretAccessKey: randomHex(20),
		}
		user.keys[name] = key

		return nil
	})

	return key, err
}

func (f *Fake) ListKeys(_ context.Context, ref bucket.Ref) ([]bucket.KeyInfo, error) {
	var keys []bucket.KeyInfo
	err := f.with("ListKeys", ref, func(user *fakeUser) error {
		keys = make([]bucket.KeyInfo, 0, len(user.keys))
		for _, key := range user.keys {
			keys = append(keys, key.KeyInfo)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Name < keys[j].Name })
		return nil
	})

	return keys, err
}

func (f *Fake) RevokeKey(_ context.Context, ref bucket.Ref, name string) error {
	return f.with("RevokeKey", ref, func(user *fakeUser) error {
		if _, ok := user.keys[name]; !ok {
			return bucket.ErrKeyNotFound
		}
		delete(user.keys, name)
		return nil
	})
}

func (f *Fake) Usage(_ context.Context, ref bucket.Ref) (bucket.Usage, error) {
	var usage bucket.Usage
	err := f.with("Usage", ref, func(user *fakeUser) error {
		usage = user.usage()
		return nil
	})

	return usage, err
}

func (f *Fake) Put(_ context.Context, ref bucket.Ref, key bucket.Key, object string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	user, err := f.authorize(ref, key)
	if err != nil {
		return err
	}
	if key.Access != bucket.KeyReadWrite {
		return fmt.Errorf("%w: key %s is read-only", ErrRefused, key.Name)
	}
	if used := user.usage().UsedBytes - int64(len(user.objects[object])); used+int64(len(body)) > user.quota {
		return fmt.Errorf("%w: quota exceeded", ErrRefused)
	}

	user.objects[object] = append([]byte(nil), body...)

	return nil
}

func (f *Fake) Get(_ context.Context, ref bucket.Ref, key bucket.Key, object string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	user, err := f.authorize(ref, key)
	if err != nil {
		return nil, err
	}

	body, ok := user.objects[object]
	if !ok {
		return nil, fmt.Errorf("%w: no object %s", ErrRefused, object)
	}

	return append([]byte(nil), body...), nil
}

// authorize finds the bucket a key opens, as an S3 server would: by the key
// pair alone, refusing keys of suspended users.
func (f *Fake) authorize(ref bucket.Ref, key bucket.Key) (*fakeUser, error) {
	owner, ok := f.buckets[ref.Bucket]
	if !ok {
		return nil, fmt.Errorf("%w: no bucket %s", ErrRefused, ref.Bucket)
	}
	user := f.users[owner]

	held, ok := user.keys[key.Name]
	if !ok || held.AccessKeyID != key.AccessKeyID || held.SecretAccessKey != key.SecretAccessKey {
		return nil, fmt.Errorf("%w: unknown key", ErrRefused)
	}
	if user.suspended {
		return nil, fmt.Errorf("%w: user suspended", ErrRefused)
	}

	return user, nil
}

// user finds ref's user, refusing one that belongs to another volume.
func (f *Fake) user(ref bucket.Ref) (*fakeUser, error) {
	user, ok := f.users[ref.UserID]
	if !ok {
		return nil, bucket.ErrNotFound
	}
	if user.owner != ref.Owner || user.vid != ref.Vid {
		return nil, fmt.Errorf("%w: user %s", bucket.ErrConflict, ref.UserID)
	}

	return user, nil
}

func (f *Fake) with(method string, ref bucket.Ref, fn func(*fakeUser) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs[method]; err != nil {
		return err
	}

	user, err := f.user(ref)
	if err != nil {
		return err
	}
	if user.bucket != ref.Bucket {
		return fmt.Errorf("%w: bucket %s", bucket.ErrConflict, ref.Bucket)
	}

	return fn(user)
}

func (u *fakeUser) usage() bucket.Usage {
	var usage bucket.Usage
	for _, body := range u.objects {
		usage.UsedBytes += int64(len(body))
		usage.Objects++
	}

	return usage
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}

	return hex.EncodeToString(buf)
}
