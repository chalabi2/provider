package buckettest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/akash-network/provider/bucket"
)

// DataPlane reaches a bucket's objects with a tenant key, as a workload does.
// It returns an error for any request the S3 server refuses.
type DataPlane interface {
	Put(ctx context.Context, ref bucket.Ref, key bucket.Key, object string, body []byte) error
	Get(ctx context.Context, ref bucket.Ref, key bucket.Key, object string) ([]byte, error)
}

const (
	settle = 15 * time.Second
	tick   = 250 * time.Millisecond
)

// Conformance checks a backend against the bucket.Backend contract, through
// its admin side and through data, which reaches the same buckets with
// tenant keys.
func Conformance(t *testing.T, backend bucket.Backend, data DataPlane) {
	ctx := context.Background()

	t.Run("provisions idempotently", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		require.NoError(t, backend.Ensure(ctx, spec))

		usage, err := backend.Usage(ctx, spec.Ref)
		require.NoError(t, err)
		assert.Equal(t, bucket.Usage{}, usage)

		keys, err := backend.ListKeys(ctx, spec.Ref)
		require.NoError(t, err)
		assert.Empty(t, keys)
	})

	t.Run("keys open the bucket at their access level", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		writer, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		reader, err := backend.CreateKey(ctx, spec.Ref, "reader", bucket.KeyRead)
		require.NoError(t, err)

		assert.NotEmpty(t, writer.AccessKeyID)
		assert.NotEmpty(t, writer.SecretAccessKey)
		assert.NotEqual(t, writer.AccessKeyID, reader.AccessKeyID)
		assert.Equal(t, bucket.KeyInfo{Name: "app", Access: bucket.KeyReadWrite, AccessKeyID: writer.AccessKeyID}, writer.KeyInfo)

		keys, err := backend.ListKeys(ctx, spec.Ref)
		require.NoError(t, err)
		assert.ElementsMatch(t, []bucket.KeyInfo{writer.KeyInfo, reader.KeyInfo}, keys)

		require.NoError(t, data.Put(ctx, spec.Ref, writer, "hello.txt", []byte("hello")))
		body, err := data.Get(ctx, spec.Ref, reader, "hello.txt")
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), body)
		assert.Error(t, data.Put(ctx, spec.Ref, reader, "nope.txt", []byte("x")), "a read key writes")

		require.NoError(t, backend.RevokeKey(ctx, spec.Ref, "reader"))
		assert.Eventually(t, func() bool {
			_, err := data.Get(ctx, spec.Ref, reader, "hello.txt")
			return err != nil
		}, settle, tick, "a revoked key still reads")

		keys, err = backend.ListKeys(ctx, spec.Ref)
		require.NoError(t, err)
		assert.Equal(t, []bucket.KeyInfo{writer.KeyInfo}, keys)

		assert.ErrorIs(t, backend.RevokeKey(ctx, spec.Ref, "reader"), bucket.ErrKeyNotFound)
		assert.ErrorIs(t, backend.RevokeKey(ctx, spec.Ref, "Not_A_Key"), bucket.ErrKeyNotFound)
	})

	t.Run("keys open only their own bucket", func(t *testing.T) {
		mine := newSpec(t, backend, 1<<20)
		theirs := newSpec(t, backend, 1<<20)

		myKey, err := backend.CreateKey(ctx, mine.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		theirKey, err := backend.CreateKey(ctx, theirs.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		require.NoError(t, data.Put(ctx, theirs.Ref, theirKey, "theirs.txt", []byte("theirs")))

		_, err = data.Get(ctx, theirs.Ref, myKey, "theirs.txt")
		assert.Error(t, err, "a key reads another bucket")
		assert.Error(t, data.Put(ctx, theirs.Ref, myKey, "mine.txt", []byte("x")), "a key writes another bucket")
	})

	t.Run("refuses bad keys", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		_, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)

		_, err = backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyRead)
		assert.ErrorIs(t, err, bucket.ErrKeyExists)

		for _, name := range []string{"", "App", "app_1", "-app", "app-", "a:b"} {
			_, err = backend.CreateKey(ctx, spec.Ref, name, bucket.KeyRead)
			assert.ErrorIs(t, err, bucket.ErrInvalidKeyName, name)
		}

		for _, access := range []bucket.KeyAccess{"", "full", "write", "read-write"} {
			_, err = backend.CreateKey(ctx, spec.Ref, "other", access)
			assert.ErrorIs(t, err, bucket.ErrInvalidAccess, access)
		}
	})

	t.Run("limits the number of keys", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		for i := range bucket.MaxKeys {
			_, err := backend.CreateKey(ctx, spec.Ref, keyName(i), bucket.KeyRead)
			require.NoError(t, err)
		}

		_, err := backend.CreateKey(ctx, spec.Ref, "one-too-many", bucket.KeyRead)
		assert.ErrorIs(t, err, bucket.ErrTooManyKeys)
	})

	t.Run("enforces and grows the quota", func(t *testing.T) {
		spec := newSpec(t, backend, 64<<10)

		key, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)

		big := bytes.Repeat([]byte("x"), 128<<10)
		assert.Error(t, data.Put(ctx, spec.Ref, key, "big", big), "a write over the quota succeeds")
		require.NoError(t, data.Put(ctx, spec.Ref, key, "small", []byte("small")))

		assert.Eventually(t, func() bool {
			usage, err := backend.Usage(ctx, spec.Ref)
			return err == nil && usage.Objects == 1 && usage.UsedBytes >= int64(len("small"))
		}, settle, tick, "usage does not show the object")

		spec.SizeBytes = 1 << 20
		require.NoError(t, backend.Ensure(ctx, spec))
		assert.Eventually(t, func() bool {
			return data.Put(ctx, spec.Ref, key, "big", big) == nil
		}, settle, tick, "the grown quota is not applied")
	})

	t.Run("suspends every key and resumes with none", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		old, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		require.NoError(t, data.Put(ctx, spec.Ref, old, "kept.txt", []byte("kept")))

		require.NoError(t, backend.Suspend(ctx, spec.Ref))
		assert.Eventually(t, func() bool {
			_, err := data.Get(ctx, spec.Ref, old, "kept.txt")
			return err != nil
		}, settle, tick, "a suspended bucket's key still reads")
		assert.Error(t, data.Put(ctx, spec.Ref, old, "late.txt", []byte("x")), "a suspended bucket's key writes")

		require.NoError(t, backend.Resume(ctx, spec.Ref))

		keys, err := backend.ListKeys(ctx, spec.Ref)
		require.NoError(t, err)
		assert.Empty(t, keys, "keys survive a resume")

		_, err = data.Get(ctx, spec.Ref, old, "kept.txt")
		assert.Error(t, err, "a key of the previous lease reads after a resume")

		fresh, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyRead)
		require.NoError(t, err)
		assert.Eventually(t, func() bool {
			body, err := data.Get(ctx, spec.Ref, fresh, "kept.txt")
			return err == nil && string(body) == "kept"
		}, settle, tick, "a resumed bucket lost its data")
	})

	t.Run("seals every key, and only the provider reaches the objects", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		key, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		require.NoError(t, data.Put(ctx, spec.Ref, key, "kept.txt", []byte("kept")))

		require.NoError(t, backend.Seal(ctx, spec.Ref))
		require.NoError(t, backend.Seal(ctx, spec.Ref), "a second seal fails")

		keys, err := backend.ListKeys(ctx, spec.Ref)
		require.NoError(t, err)
		assert.Empty(t, keys, "keys survive a seal")

		assert.Eventually(t, func() bool {
			_, err := data.Get(ctx, spec.Ref, key, "kept.txt")
			return err != nil
		}, settle, tick, "a sealed bucket's key still reads")
		assert.Error(t, data.Put(ctx, spec.Ref, key, "late.txt", []byte("x")), "a sealed bucket's key writes")

		objects := openObjects(t, backend, spec.Ref)

		listed, more, err := objects.List(ctx, "", 0)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Equal(t, []bucket.ObjectInfo{{Key: "kept.txt", Size: 4}}, listed)

		_, body := getObject(t, objects, "kept.txt")
		assert.Equal(t, []byte("kept"), body)
	})

	t.Run("moves objects whole, as they are served", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)
		objects := openObjects(t, backend, spec.Ref)

		keys := []string{
			"plain.txt",
			"dir/nested/file.bin",
			"with space+plus&and=eq?q#h;semi,comma.txt",
			"ünïcødé/文件.txt",
			"percent%2Fliteral",
			"tilde~star*paren(1)!'$@:.txt",
			"empty",
		}

		written := make(map[string]bucket.ObjectInfo, len(keys))
		for i, key := range keys {
			body := bytes.Repeat([]byte{byte('a' + i)}, i*100)
			info := bucket.ObjectInfo{
				Key:                key,
				Size:               int64(len(body)),
				ContentType:        "text/plain; charset=utf-8",
				ContentEncoding:    "identity",
				ContentDisposition: `attachment; filename="file.txt"`,
				ContentLanguage:    "en",
				CacheControl:       "max-age=60",
				Expires:            "Thu, 01 Dec 2050 16:00:00 GMT",
				Metadata:           map[string]string{"origin": "conformance", "multi-word": fmt.Sprint(i)},
			}

			putObject(t, objects, info, body)
			written[key] = info
		}

		var listed []string
		for after, more := "", true; more; {
			var page []bucket.ObjectInfo
			var err error

			page, more, err = objects.List(ctx, after, 2)
			require.NoError(t, err)
			require.LessOrEqual(t, len(page), 2)

			for _, object := range page {
				assert.Equal(t, written[object.Key].Size, object.Size, object.Key)
				listed = append(listed, object.Key)
				after = object.Key
			}
		}

		sorted := append([]string(nil), keys...)
		sort.Strings(sorted)
		assert.Equal(t, sorted, listed, "a listing is not every key in order")

		for i, key := range keys {
			info, body := getObject(t, objects, key)
			assert.Equal(t, written[key], info, key)
			assert.Equal(t, bytes.Repeat([]byte{byte('a' + i)}, i*100), body, key)
		}

		require.NoError(t, objects.Delete(ctx, "plain.txt"))
		require.NoError(t, objects.Delete(ctx, "plain.txt"), "deleting a missing object fails")

		_, _, err := objects.Get(ctx, "plain.txt")
		assert.ErrorIs(t, err, bucket.ErrObjectNotFound)

		body := []byte("not what the digest says")
		wrong := sha256.Sum256([]byte("something else"))
		assert.Error(t, objects.Put(ctx, bucket.ObjectInfo{Key: "corrupt", Size: int64(len(body))}, bytes.NewReader(body), wrong[:]), "an object that does not match its digest is written")

		_, _, err = objects.Get(ctx, "corrupt")
		assert.ErrorIs(t, err, bucket.ErrObjectNotFound, "an object that does not match its digest is kept")
	})

	t.Run("the provider's writes stay within the quota", func(t *testing.T) {
		spec := newSpec(t, backend, 64<<10)
		objects := openObjects(t, backend, spec.Ref)

		big := bytes.Repeat([]byte("x"), 128<<10)
		sum := sha256.Sum256(big)
		assert.Error(t, objects.Put(ctx, bucket.ObjectInfo{Key: "big", Size: int64(len(big))}, bytes.NewReader(big), sum[:]), "a write over the quota succeeds")
	})

	t.Run("deletes the bucket, its data and its keys", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		key, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		require.NoError(t, data.Put(ctx, spec.Ref, key, "gone.txt", []byte("gone")))

		require.NoError(t, backend.Delete(ctx, spec.Ref))
		require.NoError(t, backend.Delete(ctx, spec.Ref), "a second delete fails")

		assertMissing(t, backend, spec.Ref)

		_, err = data.Get(ctx, spec.Ref, key, "gone.txt")
		assert.Error(t, err, "a deleted bucket's key reads")
	})

	t.Run("reports a missing bucket", func(t *testing.T) {
		assertMissing(t, backend, newRef(t))
	})

	t.Run("never hands over another volume's bucket", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		key, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		require.NoError(t, data.Put(ctx, spec.Ref, key, "mine.txt", []byte("mine")))

		other := newRef(t)
		other.Bucket = spec.Bucket

		assert.ErrorIs(t, backend.Ensure(ctx, bucket.Spec{Ref: other, SizeBytes: 1 << 20}), bucket.ErrConflict)
		assert.NoError(t, backend.Delete(ctx, other))

		body, err := data.Get(ctx, spec.Ref, key, "mine.txt")
		require.NoError(t, err)
		assert.Equal(t, []byte("mine"), body)
	})

	t.Run("never hands over another volume's user", func(t *testing.T) {
		spec := newSpec(t, backend, 1<<20)

		key, err := backend.CreateKey(ctx, spec.Ref, "app", bucket.KeyReadWrite)
		require.NoError(t, err)
		require.NoError(t, data.Put(ctx, spec.Ref, key, "mine.txt", []byte("mine")))

		// a volume whose user id is the same truncated hash
		other := spec.Ref
		other.Owner = "akash1intruder"
		other.Vid = "intruder"

		assert.ErrorIs(t, backend.Ensure(ctx, bucket.Spec{Ref: other, SizeBytes: 1 << 30}), bucket.ErrConflict, "Ensure")
		_, err = backend.Usage(ctx, other)
		assert.ErrorIs(t, err, bucket.ErrConflict, "Usage")
		_, err = backend.ListKeys(ctx, other)
		assert.ErrorIs(t, err, bucket.ErrConflict, "ListKeys")
		_, err = backend.CreateKey(ctx, other, "intruder", bucket.KeyReadWrite)
		assert.ErrorIs(t, err, bucket.ErrConflict, "CreateKey")
		assert.ErrorIs(t, backend.RevokeKey(ctx, other, "app"), bucket.ErrConflict, "RevokeKey")
		assert.ErrorIs(t, backend.Suspend(ctx, other), bucket.ErrConflict, "Suspend")
		assert.ErrorIs(t, backend.Resume(ctx, other), bucket.ErrConflict, "Resume")
		assert.ErrorIs(t, backend.Seal(ctx, other), bucket.ErrConflict, "Seal")
		_, err = backend.Objects(ctx, other)
		assert.ErrorIs(t, err, bucket.ErrConflict, "Objects")
		assert.ErrorIs(t, backend.Delete(ctx, other), bucket.ErrConflict, "Delete")

		keys, err := backend.ListKeys(ctx, spec.Ref)
		require.NoError(t, err)
		assert.Equal(t, []bucket.KeyInfo{key.KeyInfo}, keys)

		body, err := data.Get(ctx, spec.Ref, key, "mine.txt")
		require.NoError(t, err)
		assert.Equal(t, []byte("mine"), body)
		require.NoError(t, data.Put(ctx, spec.Ref, key, "still.txt", []byte("writable")))
	})
}

func assertMissing(t *testing.T, backend bucket.Backend, ref bucket.Ref) {
	t.Helper()
	ctx := context.Background()

	_, err := backend.Usage(ctx, ref)
	assert.ErrorIs(t, err, bucket.ErrNotFound, "Usage")
	_, err = backend.ListKeys(ctx, ref)
	assert.ErrorIs(t, err, bucket.ErrNotFound, "ListKeys")
	_, err = backend.CreateKey(ctx, ref, "app", bucket.KeyRead)
	assert.ErrorIs(t, err, bucket.ErrNotFound, "CreateKey")
	assert.ErrorIs(t, backend.RevokeKey(ctx, ref, "app"), bucket.ErrNotFound, "RevokeKey")
	assert.ErrorIs(t, backend.Suspend(ctx, ref), bucket.ErrNotFound, "Suspend")
	assert.ErrorIs(t, backend.Resume(ctx, ref), bucket.ErrNotFound, "Resume")
	assert.ErrorIs(t, backend.Seal(ctx, ref), bucket.ErrNotFound, "Seal")
	_, err = backend.Objects(ctx, ref)
	assert.ErrorIs(t, err, bucket.ErrNotFound, "Objects")
	assert.NoError(t, backend.Delete(ctx, ref), "Delete")
}

// openObjects opens the provider's session on a bucket's objects for the
// test and closes it afterwards.
func openObjects(t *testing.T, backend bucket.Backend, ref bucket.Ref) bucket.Objects {
	t.Helper()

	objects, err := backend.Objects(context.Background(), ref)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, objects.Close())
	})

	return objects
}

func putObject(t *testing.T, objects bucket.Objects, info bucket.ObjectInfo, body []byte) {
	t.Helper()

	sum := sha256.Sum256(body)
	require.NoError(t, objects.Put(context.Background(), info, bytes.NewReader(body), sum[:]), info.Key)
}

func getObject(t *testing.T, objects bucket.Objects, key string) (bucket.ObjectInfo, []byte) {
	t.Helper()

	info, rc, err := objects.Get(context.Background(), key)
	require.NoError(t, err, key)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	body, err := io.ReadAll(rc)
	require.NoError(t, err, key)

	return info, body
}

// newSpec provisions a bucket for the test and deletes it afterwards.
func newSpec(t *testing.T, backend bucket.Backend, size int64) bucket.Spec {
	t.Helper()

	ref := newRef(t)
	spec := bucket.Spec{Ref: ref, SizeBytes: size}

	require.NoError(t, backend.Ensure(context.Background(), spec))
	t.Cleanup(func() {
		assert.NoError(t, backend.Delete(context.Background(), ref))
	})

	return spec
}

func newRef(t *testing.T) bucket.Ref {
	t.Helper()

	id := randomHex(6)
	return bucket.RefFor("volume-test-"+id, "akash1conformance"+id, "test-"+id)
}

func keyName(i int) string {
	return fmt.Sprintf("key-%02d", i)
}
