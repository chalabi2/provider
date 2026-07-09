package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	atls "pkg.akt.dev/go/util/tls"
	vtv1 "pkg.akt.dev/go/volume/v1"
)

// Dial opens the VolumeTransfer client half of the data plane against the
// source provider's gateway: mTLS with this provider's x/cert identity as
// the client certificate, and the server certificate validated against
// chain state (x/cert) and pinned to the expected source address - the
// mirror of the exporter's gate. No trust in DNS, no trust in the dialed
// endpoint beyond what the chain records.
func Dial(ctx context.Context, endpoint, source string, certs []tls.Certificate, cquery atls.CertificateQuerier) (vtv1.VolumeTransferClient, io.Closer, error) {
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: certs,
		// verification happens against on-chain x/cert state, not a CA
		// pool; the custom callback below is the actual gate
		InsecureSkipVerify: true, // nolint: gosec
		VerifyPeerCertificate: func(certificates [][]byte, _ [][]*x509.Certificate) error {
			peerCerts := make([]*x509.Certificate, 0, len(certificates))

			for idx := range certificates {
				cert, err := x509.ParseCertificate(certificates[idx])
				if err != nil {
					return err
				}

				peerCerts = append(peerCerts, cert)
			}

			owner, _, err := atls.ValidatePeerCertificates(ctx, cquery, peerCerts, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
			if err != nil {
				return err
			}

			if owner.String() != source {
				return fmt.Errorf("%w: dialed %q but the certificate belongs to %q", ErrNotAuthorized, source, owner.String())
			}

			return nil
		},
	}

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, nil, err
	}

	return vtv1.NewVolumeTransferClient(conn), conn, nil
}
