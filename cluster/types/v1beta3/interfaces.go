package v1beta3

import (
	mani "pkg.akt.dev/go/manifest/v2beta4"
)

type MGroup interface {
	ManifestGroup() mani.Group
}
