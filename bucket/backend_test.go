package bucket

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateKeyName(t *testing.T) {
	for _, name := range []string{"a", "app", "app-1", "0", strings.Repeat("a", 32)} {
		assert.NoError(t, ValidateKeyName(name), name)
	}

	for _, name := range []string{"", "App", "app_1", "-app", "app-", "a:b", "a/b", "a.b", strings.Repeat("a", 33)} {
		assert.ErrorIs(t, ValidateKeyName(name), ErrInvalidKeyName, name)
	}
}

func TestValidateKeyAccess(t *testing.T) {
	assert.NoError(t, ValidateKeyAccess(KeyReadWrite))
	assert.NoError(t, ValidateKeyAccess(KeyRead))

	for _, access := range []KeyAccess{"", "full", "write", "read-write", "READ"} {
		assert.ErrorIs(t, ValidateKeyAccess(access), ErrInvalidAccess, access)
	}
}

// S3 bucket names: 3 to 63 lowercase letters, digits and hyphens, starting
// and ending with a letter or digit.
var s3BucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func TestRefFor(t *testing.T) {
	ref := RefFor("volume-0123456789ab", "akash1owner", "pgdata")

	// pinned: a new scheme would lose every existing bucket
	assert.Equal(t, Ref{
		UserID: "volume-0123456789ab",
		Bucket: "pgdata-a92f2055a384ecf9",
		Owner:  "akash1owner",
		Vid:    "pgdata",
	}, ref)
	assert.Regexp(t, s3BucketName, ref.Bucket)
	assert.Equal(t, ref, RefFor("volume-0123456789ab", "akash1owner", "pgdata"), "not deterministic")

	assert.NotEqual(t, ref.Bucket, RefFor(ref.UserID, "akash1other", "pgdata").Bucket, "owners share a bucket")
}

func TestRefForLongVid(t *testing.T) {
	long := strings.Repeat("a", 45) + "-" + strings.Repeat("b", 17)

	ref := RefFor("volume-0123456789ab", "akash1owner", long)

	assert.LessOrEqual(t, len(ref.Bucket), 63)
	assert.Regexp(t, s3BucketName, ref.Bucket)
	assert.True(t, strings.HasPrefix(ref.Bucket, strings.Repeat("a", 45)+"-"), ref.Bucket)
	assert.NotContains(t, ref.Bucket, "--", "a cut vid keeps its trailing hyphen")

	// vids that share the kept prefix still get their own buckets
	other := RefFor("volume-0123456789ab", "akash1owner", strings.Repeat("a", 45)+"-"+strings.Repeat("c", 17))
	assert.NotEqual(t, ref.Bucket, other.Bucket)
}
