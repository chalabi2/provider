package bbolt

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	ctypes "pkg.akt.dev/go/node/cert/v1"

	"github.com/akash-network/provider/tools/pconfig"
	"github.com/akash-network/provider/verification/inventory"
)

var (
	ErrUninitialized            = errors.New("bbolt: un-initialized database")
	ErrPubKeyEntry              = errors.New("bbolt: account does not contain pubkey")
	ErrDBClosed                 = errors.New("bbolt: database instance closed")
	ErrCertificateNotFoundInPEM = fmt.Errorf("%w: certificate not found in PEM", ctypes.ErrCertificate)
	ErrInvalidPubKeyType        = errors.New("bbolt: invalid pubkey type")
	errInvalidSnapshotRecord    = errors.New("bbolt: invalid committed inventory snapshot record")
)

var (
	bucketAccounts                 = []byte("accounts")
	bucketBidEngine                = []byte("bidengine")
	bucketInventorySnapshots       = []byte("inventory-snapshots")
	bucketInventorySnapshotRecords = []byte("records")
	keyPubKey                      = []byte("pubkey")
	bucketCertificates             = []byte("certificates")
	keyCertificate                 = []byte("certificate")
	keyNextKey                     = []byte("nextkey")
	keyInventorySnapshotHash       = []byte("hash")
	keyInventorySnapshotPayload    = []byte("payload")
	keyInventorySnapshotSignature  = []byte("signature")
	keyInventorySnapshotProvider   = []byte("provider")
	keyInventorySnapshotState      = []byte("state")
	keyInventorySnapshotPostedAt   = []byte("posted-at")
	keyLatestInventorySnapshot     = []byte("latest-posted")
)

type impl struct {
	db     *bbolt.DB
	closed chan struct{}
}

var _ pconfig.Storage = (*impl)(nil)

func NewBBolt(path string) (pconfig.Storage, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketAccounts)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		_, err = tx.CreateBucketIfNotExists(bucketBidEngine)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		snapshots, err := tx.CreateBucketIfNotExists(bucketInventorySnapshots)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		_, err = snapshots.CreateBucketIfNotExists(bucketInventorySnapshotRecords)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	b := &impl{
		db:     db,
		closed: make(chan struct{}),
	}

	return b, nil
}

func (b *impl) BidEngine() pconfig.BidEngine {
	return b
}

func (b *impl) InventorySnapshots() inventory.CommittedSnapshotStore {
	return b
}

func (b *impl) Close() error {
	select {
	case <-b.closed:
		return nil
	default:
	}

	close(b.closed)

	return b.db.Close()
}

func (b *impl) AddAccount(_ context.Context, acc sdk.Address, pubkey cryptotypes.PubKey) error {
	select {
	case <-b.closed:
		return ErrDBClosed
	default:
	}

	if acc.Empty() || pubkey == nil {
		return pconfig.ErrInvalidArgs
	}

	err := b.db.Update(func(tx *bbolt.Tx) error {
		accounts := tx.Bucket(bucketAccounts)
		if accounts == nil {
			return ErrUninitialized
		}

		account, err := accounts.CreateBucket(acc.Bytes())
		if err != nil {
			if errors.Is(err, berrors.ErrBucketExists) {
				return pconfig.ErrAccountExists
			}

			return err
		}

		err = account.Put(keyPubKey, pubkey.Bytes())
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (b *impl) DelAccount(_ context.Context, acc sdk.Address) error {
	if acc.Empty() {
		return pconfig.ErrInvalidArgs
	}

	err := b.db.Update(func(tx *bbolt.Tx) error {
		accounts := tx.Bucket(bucketAccounts)
		if accounts == nil {
			return ErrUninitialized
		}

		err := accounts.DeleteBucket(acc.Bytes())
		if err != nil {
			if errors.Is(err, berrors.ErrBucketExists) {
				return pconfig.ErrAccountNotExists
			}

			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (b *impl) GetAccountPublicKey(_ context.Context, acc sdk.Address) (cryptotypes.PubKey, error) {
	var res cryptotypes.PubKey

	if acc.Empty() {
		return nil, pconfig.ErrInvalidArgs
	}

	err := b.db.View(func(tx *bbolt.Tx) error {
		accounts := tx.Bucket(bucketAccounts)
		if accounts == nil {
			return ErrUninitialized
		}

		account := accounts.Bucket(acc.Bytes())
		if account == nil {
			return pconfig.ErrAccountNotExists
		}

		pubkey := account.Get(keyPubKey)
		res = &secp256k1.PubKey{Key: pubkey}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (b *impl) AddAccountCertificate(_ context.Context, acc sdk.Address, cert *x509.Certificate, pubkey crypto.PublicKey) error {
	if acc.Empty() || cert == nil || pubkey == nil {
		return pconfig.ErrInvalidArgs
	}

	err := b.db.Update(func(tx *bbolt.Tx) error {
		accounts := tx.Bucket(bucketAccounts)
		if accounts == nil {
			return ErrUninitialized
		}

		account := accounts.Bucket(acc.Bytes())
		if account == nil {
			return pconfig.ErrAccountNotExists
		}

		certs, err := account.CreateBucketIfNotExists(bucketCertificates)
		if err != nil {
			return err
		}

		certBucket, err := certs.CreateBucket(cert.SerialNumber.Bytes())
		if err != nil {
			if errors.Is(err, berrors.ErrBucketExists) {
				return pconfig.ErrCertificateExists
			}
			return err
		}
		certData := pem.EncodeToMemory(&pem.Block{Type: ctypes.PemBlkTypeCertificate, Bytes: cert.Raw})

		err = certBucket.Put(keyCertificate, certData)
		if err != nil {
			return err
		}

		pk, err := x509.MarshalPKIXPublicKey(pubkey)
		if err != nil {
			return fmt.Errorf("%w: failed extracting public key", err)
		}

		err = certBucket.Put(keyPubKey, pk)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (b *impl) DelAccountCertificate(_ context.Context, acc sdk.Address, serial *big.Int) error {
	if acc.Empty() || serial == nil {
		return pconfig.ErrInvalidArgs
	}

	err := b.db.Update(func(tx *bbolt.Tx) error {
		accounts := tx.Bucket(bucketAccounts)
		if accounts == nil {
			return ErrUninitialized
		}

		account := accounts.Bucket(acc.Bytes())
		if account == nil {
			return pconfig.ErrAccountNotExists
		}

		certs := account.Bucket(bucketCertificates)
		if certs == nil {
			return pconfig.ErrCertificateNotExists
		}

		err := certs.DeleteBucket(serial.Bytes())
		if err != nil {
			if errors.Is(err, berrors.ErrBucketNotFound) {
				return pconfig.ErrCertificateNotExists
			}

			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (b *impl) GetAccountCertificate(_ context.Context, acc sdk.Address, serial *big.Int) (*x509.Certificate, crypto.PublicKey, error) {
	if acc.Empty() || serial == nil {
		return nil, nil, pconfig.ErrInvalidArgs
	}

	var cert *x509.Certificate
	var pubKey crypto.PublicKey

	err := b.db.View(func(tx *bbolt.Tx) error {
		accounts := tx.Bucket(bucketAccounts)
		if accounts == nil {
			return ErrUninitialized
		}

		account := accounts.Bucket(acc.Bytes())
		if account == nil {
			return pconfig.ErrAccountNotExists
		}

		certs := account.Bucket(bucketCertificates)
		if certs == nil {
			return pconfig.ErrCertificateNotExists
		}

		certBucket := certs.Bucket(serial.Bytes())
		if certBucket == nil {
			return pconfig.ErrCertificateNotExists
		}

		block, _ := pem.Decode(certBucket.Get(keyCertificate))
		if block == nil {
			return ErrCertificateNotFoundInPEM
		}

		var err error
		cert, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}

		pk, err := x509.ParsePKIXPublicKey(certBucket.Get(keyPubKey))
		if err != nil {
			return err
		}

		var valid bool
		pubKey, valid = pk.(*ecdsa.PublicKey)
		if !valid {
			return ErrInvalidPubKeyType
		}

		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return cert, pubKey, nil
}

func (b *impl) GetAllCertificates(_ context.Context) ([]*x509.Certificate, error) {
	res := make([]*x509.Certificate, 0)

	err := b.db.View(func(tx *bbolt.Tx) error {
		accountsBucket := tx.Bucket(bucketAccounts)
		if accountsBucket == nil {
			return ErrUninitialized
		}

		return accountsBucket.ForEachBucket(func(k []byte) error {
			account := accountsBucket.Bucket(k)

			certsBucket := account.Bucket(bucketCertificates)
			if certsBucket == nil {
				return nil
			}

			return certsBucket.ForEachBucket(func(k []byte) error {
				certBucket := certsBucket.Bucket(k)

				block, _ := pem.Decode(certBucket.Get(keyCertificate))
				if block == nil {
					return ErrCertificateNotFoundInPEM
				}

				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return err
				}

				res = append(res, cert)

				return nil
			})
		})
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (b *impl) SetOrdersNextKey(_ context.Context, nextkey []byte) error {
	select {
	case <-b.closed:
		return ErrDBClosed
	default:
	}

	err := b.db.Update(func(tx *bbolt.Tx) error {
		bidengine := tx.Bucket(bucketBidEngine)
		if bidengine == nil {
			return ErrUninitialized
		}

		err := bidengine.Put(keyNextKey, nextkey)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (b *impl) GetOrdersNextKey(_ context.Context) ([]byte, error) {
	var res []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		bidengine := tx.Bucket(bucketBidEngine)
		if bidengine == nil {
			return ErrUninitialized
		}

		val := bidengine.Get(keyNextKey)

		res = make([]byte, len(val))
		copy(res, val)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (b *impl) Stage(_ context.Context, snapshot inventory.Snapshot) error {
	if b.isClosed() {
		return ErrDBClosed
	}

	record, err := inventory.NewPendingCommittedSnapshot(snapshot)
	if err != nil {
		return err
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		records, err := inventorySnapshotRecordsBucket(tx)
		if err != nil {
			return err
		}

		recordBucket := records.Bucket(record.Snapshot.Hash)
		if recordBucket != nil {
			current, err := readInventorySnapshotRecord(recordBucket, record.Snapshot.Hash)
			if err != nil {
				return err
			}
			if !inventorySnapshotsEqual(current.Snapshot, record.Snapshot) {
				return inventory.ErrCommittedSnapshotConflict
			}

			return nil
		}

		recordBucket, err = records.CreateBucket(record.Snapshot.Hash)
		if err != nil {
			return err
		}

		return writeInventorySnapshotRecord(recordBucket, record)
	})
}

func (b *impl) Pending(_ context.Context) ([]inventory.CommittedSnapshot, error) {
	if b.isClosed() {
		return nil, ErrDBClosed
	}

	records := make([]inventory.CommittedSnapshot, 0)
	err := b.db.View(func(tx *bbolt.Tx) error {
		recordsBucket, err := inventorySnapshotRecordsBucket(tx)
		if err != nil {
			return err
		}

		return recordsBucket.ForEachBucket(func(hash []byte) error {
			record, err := readInventorySnapshotRecord(recordsBucket.Bucket(hash), hash)
			if err != nil {
				return err
			}
			if record.State == inventory.CommittedSnapshotStatePending {
				records = append(records, record)
			}

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(records, func(lhs, rhs int) bool {
		return bytes.Compare(records[lhs].Snapshot.Hash, records[rhs].Snapshot.Hash) < 0
	})

	return records, nil
}

func (b *impl) MarkPosted(_ context.Context, hash []byte, postedAt time.Time) error {
	if b.isClosed() {
		return ErrDBClosed
	}
	if err := inventory.ValidateCommittedSnapshotHash(hash); err != nil {
		return err
	}
	if postedAt.IsZero() {
		return pconfig.ErrInvalidArgs
	}
	postedAt = postedAt.Round(0).UTC()

	return b.db.Update(func(tx *bbolt.Tx) error {
		snapshots := tx.Bucket(bucketInventorySnapshots)
		if snapshots == nil {
			return ErrUninitialized
		}
		records := snapshots.Bucket(bucketInventorySnapshotRecords)
		if records == nil {
			return ErrUninitialized
		}

		recordBucket := records.Bucket(hash)
		if recordBucket == nil {
			return inventory.ErrCommittedSnapshotNotFound
		}

		record, err := readInventorySnapshotRecord(recordBucket, hash)
		if err != nil {
			return err
		}
		if record.State == inventory.CommittedSnapshotStatePosted {
			if record.PostedAt.Equal(postedAt) {
				return nil
			}

			return inventory.ErrCommittedSnapshotConflict
		}

		shouldUpdateLatest := true
		latestHash := snapshots.Get(keyLatestInventorySnapshot)
		if latestHash != nil {
			if err := inventory.ValidateCommittedSnapshotHash(latestHash); err != nil {
				return fmt.Errorf("%w: latest hash: %s", errInvalidSnapshotRecord, err)
			}

			latestBucket := records.Bucket(latestHash)
			if latestBucket == nil {
				return fmt.Errorf("%w: latest snapshot does not exist", errInvalidSnapshotRecord)
			}

			latest, err := readInventorySnapshotRecord(latestBucket, latestHash)
			if err != nil {
				return err
			}
			if latest.State != inventory.CommittedSnapshotStatePosted {
				return fmt.Errorf("%w: latest snapshot is not posted", errInvalidSnapshotRecord)
			}

			shouldUpdateLatest = !postedAt.Before(latest.PostedAt)
		}

		record, err = inventory.MarkCommittedSnapshotPosted(record, postedAt)
		if err != nil {
			return err
		}
		if err := writeInventorySnapshotRecord(recordBucket, record); err != nil {
			return err
		}
		if shouldUpdateLatest {
			return snapshots.Put(keyLatestInventorySnapshot, record.Snapshot.Hash)
		}

		return nil
	})
}

func (b *impl) Get(_ context.Context, hash []byte) (inventory.CommittedSnapshot, error) {
	if b.isClosed() {
		return inventory.CommittedSnapshot{}, ErrDBClosed
	}
	if err := inventory.ValidateCommittedSnapshotHash(hash); err != nil {
		return inventory.CommittedSnapshot{}, err
	}

	var record inventory.CommittedSnapshot
	err := b.db.View(func(tx *bbolt.Tx) error {
		records, err := inventorySnapshotRecordsBucket(tx)
		if err != nil {
			return err
		}

		recordBucket := records.Bucket(hash)
		if recordBucket == nil {
			return inventory.ErrCommittedSnapshotNotFound
		}

		record, err = readInventorySnapshotRecord(recordBucket, hash)
		if err != nil {
			return err
		}
		if record.State != inventory.CommittedSnapshotStatePosted {
			return inventory.ErrCommittedSnapshotNotFound
		}

		return nil
	})
	if err != nil {
		return inventory.CommittedSnapshot{}, err
	}

	return record, nil
}

func (b *impl) Latest(_ context.Context) (inventory.CommittedSnapshot, error) {
	if b.isClosed() {
		return inventory.CommittedSnapshot{}, ErrDBClosed
	}

	var record inventory.CommittedSnapshot
	err := b.db.View(func(tx *bbolt.Tx) error {
		snapshots := tx.Bucket(bucketInventorySnapshots)
		if snapshots == nil {
			return ErrUninitialized
		}

		hash := snapshots.Get(keyLatestInventorySnapshot)
		if hash == nil {
			return inventory.ErrCommittedSnapshotNotFound
		}
		if err := inventory.ValidateCommittedSnapshotHash(hash); err != nil {
			return fmt.Errorf("%w: latest hash: %s", errInvalidSnapshotRecord, err)
		}

		records := snapshots.Bucket(bucketInventorySnapshotRecords)
		if records == nil {
			return ErrUninitialized
		}
		recordBucket := records.Bucket(hash)
		if recordBucket == nil {
			return fmt.Errorf("%w: latest snapshot does not exist", errInvalidSnapshotRecord)
		}

		var err error
		record, err = readInventorySnapshotRecord(recordBucket, hash)
		if err != nil {
			return err
		}
		if record.State != inventory.CommittedSnapshotStatePosted {
			return fmt.Errorf("%w: latest snapshot is not posted", errInvalidSnapshotRecord)
		}

		return nil
	})
	if err != nil {
		return inventory.CommittedSnapshot{}, err
	}

	return record, nil
}

func (b *impl) isClosed() bool {
	select {
	case <-b.closed:
		return true
	default:
		return false
	}
}

func inventorySnapshotRecordsBucket(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	snapshots := tx.Bucket(bucketInventorySnapshots)
	if snapshots == nil {
		return nil, ErrUninitialized
	}

	records := snapshots.Bucket(bucketInventorySnapshotRecords)
	if records == nil {
		return nil, ErrUninitialized
	}

	return records, nil
}

func writeInventorySnapshotRecord(bucket *bbolt.Bucket, record inventory.CommittedSnapshot) error {
	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return err
	}

	if err := bucket.Put(keyInventorySnapshotHash, record.Snapshot.Hash); err != nil {
		return err
	}
	if err := bucket.Put(keyInventorySnapshotPayload, record.Snapshot.Payload); err != nil {
		return err
	}
	if err := bucket.Put(keyInventorySnapshotSignature, record.Snapshot.Signature); err != nil {
		return err
	}
	if err := bucket.Put(keyInventorySnapshotProvider, []byte(record.Snapshot.Provider)); err != nil {
		return err
	}
	if err := bucket.Put(keyInventorySnapshotState, []byte{byte(record.State)}); err != nil {
		return err
	}

	if record.PostedAt.IsZero() {
		return bucket.Delete(keyInventorySnapshotPostedAt)
	}

	postedAt, err := record.PostedAt.MarshalBinary()
	if err != nil {
		return err
	}

	return bucket.Put(keyInventorySnapshotPostedAt, postedAt)
}

func readInventorySnapshotRecord(bucket *bbolt.Bucket, hash []byte) (inventory.CommittedSnapshot, error) {
	if bucket == nil {
		return inventory.CommittedSnapshot{}, fmt.Errorf("%w: missing record bucket", errInvalidSnapshotRecord)
	}
	if err := inventory.ValidateCommittedSnapshotHash(hash); err != nil {
		return inventory.CommittedSnapshot{}, fmt.Errorf("%w: bucket hash: %s", errInvalidSnapshotRecord, err)
	}

	storedHash := bucket.Get(keyInventorySnapshotHash)
	if !bytes.Equal(storedHash, hash) {
		return inventory.CommittedSnapshot{}, fmt.Errorf("%w: stored hash does not match record key", errInvalidSnapshotRecord)
	}

	state := bucket.Get(keyInventorySnapshotState)
	if len(state) != 1 {
		return inventory.CommittedSnapshot{}, fmt.Errorf("%w: state must be one byte", errInvalidSnapshotRecord)
	}

	record := inventory.CommittedSnapshot{
		Snapshot: inventory.Snapshot{
			Payload:   append([]byte(nil), bucket.Get(keyInventorySnapshotPayload)...),
			Hash:      append([]byte(nil), storedHash...),
			Signature: append([]byte(nil), bucket.Get(keyInventorySnapshotSignature)...),
			Provider:  string(bucket.Get(keyInventorySnapshotProvider)),
		},
		State: inventory.CommittedSnapshotState(state[0]),
	}

	postedAt := bucket.Get(keyInventorySnapshotPostedAt)
	if len(postedAt) != 0 {
		if err := record.PostedAt.UnmarshalBinary(postedAt); err != nil {
			return inventory.CommittedSnapshot{}, fmt.Errorf("%w: posted at: %s", errInvalidSnapshotRecord, err)
		}
		record.PostedAt = record.PostedAt.Round(0).UTC()
	}

	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return inventory.CommittedSnapshot{}, fmt.Errorf("%w: %s", errInvalidSnapshotRecord, err)
	}

	return record, nil
}

func inventorySnapshotsEqual(lhs, rhs inventory.Snapshot) bool {
	return bytes.Equal(lhs.Payload, rhs.Payload) &&
		bytes.Equal(lhs.Hash, rhs.Hash) &&
		bytes.Equal(lhs.Signature, rhs.Signature) &&
		lhs.Provider == rhs.Provider
}
