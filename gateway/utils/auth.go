package utils

import (
	"strings"

	"github.com/akash-network/provider/utils/httperror"
)

func AuthHeaderToken(headers []string) (string, error) {
	if len(headers) == 0 {
		return "", nil
	}

	if len(headers) != 1 {
		return "", httperror.ErrInvalidAuthHeader
	}

	parts := strings.Fields(headers[0])
	if len(parts) == 0 {
		return "", nil
	}
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", httperror.ErrInvalidAuthHeader
	}

	return parts[1], nil
}
