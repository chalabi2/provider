package buckettest

import (
	"testing"
)

func TestFakeConformance(t *testing.T) {
	fake := NewFake()
	Conformance(t, fake, fake)
}
