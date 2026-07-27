package memory

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"errors"
	"math/big"
	"sort"
	"sync"
	"time"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/akash-network/provider/tools/pconfig"
	"github.com/akash-network/provider/verification/inventory"
)

type certificate struct {
	cert   *x509.Certificate
	pubkey crypto.PublicKey
}

type account struct {
	pubkey cryptotypes.PubKey
	certs  map[string]certificate
}

type bidengine struct {
	nextkey []byte
}

type impl struct {
	closed chan struct{}

	lock               sync.RWMutex
	accounts           map[string]account
	bidengine          bidengine
	inventorySnapshots map[[inventory.SnapshotHashSize]byte]inventory.CommittedSnapshot
	latestSnapshot     [inventory.SnapshotHashSize]byte
	hasLatestSnapshot  bool
}

var _ pconfig.Storage = (*impl)(nil)

func NewMemory() (pconfig.Storage, error) {
	b := &impl{
		closed:             make(chan struct{}),
		accounts:           make(map[string]account),
		inventorySnapshots: make(map[[inventory.SnapshotHashSize]byte]inventory.CommittedSnapshot),
	}

	return b, nil
}

func (i *impl) BidEngine() pconfig.BidEngine {
	return i
}

func (i *impl) InventorySnapshots() inventory.CommittedSnapshotStore {
	return i
}

func (i *impl) AddAccount(_ context.Context, address sdk.Address, pubkey cryptotypes.PubKey) error {
	if address.Empty() || pubkey == nil {
		return pconfig.ErrInvalidArgs
	}

	defer i.lock.Unlock()
	i.lock.Lock()

	_, exists := i.accounts[address.String()]
	if exists {
		return pconfig.ErrAccountExists
	}

	i.accounts[address.String()] = account{
		pubkey: pubkey,
		certs:  make(map[string]certificate),
	}

	return nil
}

func (i *impl) DelAccount(_ context.Context, address sdk.Address) error {
	if address.Empty() {
		return pconfig.ErrInvalidArgs
	}

	defer i.lock.Unlock()
	i.lock.Lock()

	acc, exists := i.accounts[address.String()]
	if !exists {
		return pconfig.ErrAccountNotExists
	}

	for id := range acc.certs {
		delete(acc.certs, id)
	}

	delete(i.accounts, address.String())

	return nil
}

func (i *impl) GetAllCertificates(_ context.Context) ([]*x509.Certificate, error) {
	defer i.lock.RUnlock()
	i.lock.RLock()

	res := make([]*x509.Certificate, 0)

	for _, acc := range i.accounts {
		for _, cert := range acc.certs {
			res = append(res, cert.cert)
		}
	}

	return res, nil
}

func (i *impl) GetAccountPublicKey(_ context.Context, address sdk.Address) (cryptotypes.PubKey, error) {
	if address.Empty() {
		return nil, pconfig.ErrInvalidArgs
	}

	defer i.lock.RUnlock()
	i.lock.RLock()

	acc, exists := i.accounts[address.String()]
	if !exists {
		return nil, pconfig.ErrAccountNotExists
	}

	return acc.pubkey, nil
}

func (i *impl) GetAccountCertificate(_ context.Context, address sdk.Address, serial *big.Int) (*x509.Certificate, crypto.PublicKey, error) {
	if address.Empty() || serial == nil {
		return nil, nil, pconfig.ErrInvalidArgs
	}

	defer i.lock.RUnlock()
	i.lock.RLock()

	acc, exists := i.accounts[address.String()]
	if !exists {
		return nil, nil, pconfig.ErrAccountNotExists
	}

	cert, exists := acc.certs[serial.String()]
	if !exists {
		return nil, nil, pconfig.ErrCertificateNotExists
	}

	return cert.cert, cert.pubkey, nil
}

func (i *impl) AddAccountCertificate(_ context.Context, address sdk.Address, cert *x509.Certificate, pubkey crypto.PublicKey) error {
	if address.Empty() || cert == nil || pubkey == nil {
		return pconfig.ErrInvalidArgs
	}

	defer i.lock.Unlock()
	i.lock.Lock()

	acc, exists := i.accounts[address.String()]
	if !exists {
		return pconfig.ErrAccountNotExists
	}

	if _, exists := acc.certs[cert.SerialNumber.String()]; exists {
		return pconfig.ErrCertificateExists
	}

	acc.certs[cert.SerialNumber.String()] = certificate{
		cert:   cert,
		pubkey: pubkey,
	}

	return nil
}

func (i *impl) DelAccountCertificate(_ context.Context, address sdk.Address, serial *big.Int) error {
	if address.Empty() || serial == nil {
		return pconfig.ErrInvalidArgs
	}

	defer i.lock.Unlock()
	i.lock.Lock()

	acc, exists := i.accounts[address.String()]
	if !exists {
		return pconfig.ErrAccountNotExists
	}

	delete(acc.certs, serial.String())

	return nil
}

func (i *impl) Close() error {
	select {
	case <-i.closed:
	default:
		close(i.closed)
	}

	return nil
}

func (i *impl) SetOrdersNextKey(_ context.Context, nextkey []byte) error {
	defer i.lock.Unlock()
	i.lock.Lock()

	i.bidengine.nextkey = nextkey

	return nil
}

func (i *impl) GetOrdersNextKey(_ context.Context) ([]byte, error) {
	defer i.lock.RUnlock()
	i.lock.RLock()

	res := make([]byte, len(i.bidengine.nextkey))
	copy(res, i.bidengine.nextkey)

	return res, nil
}

func (i *impl) Stage(_ context.Context, snapshot inventory.Snapshot) error {
	record, err := inventory.NewPendingCommittedSnapshot(snapshot)
	if err != nil {
		return err
	}

	key, err := inventorySnapshotKey(record.Snapshot.Hash)
	if err != nil {
		return err
	}

	defer i.lock.Unlock()
	i.lock.Lock()

	if current, exists := i.inventorySnapshots[key]; exists {
		if err := inventory.ValidateCommittedSnapshot(current); err != nil {
			return err
		}
		if !inventorySnapshotsEqual(current.Snapshot, record.Snapshot) {
			return inventory.ErrCommittedSnapshotConflict
		}

		return nil
	}

	i.inventorySnapshots[key] = inventory.CloneCommittedSnapshot(record)

	return nil
}

func (i *impl) Pending(_ context.Context) ([]inventory.CommittedSnapshot, error) {
	defer i.lock.RUnlock()
	i.lock.RLock()

	records := make([]inventory.CommittedSnapshot, 0)
	for _, record := range i.inventorySnapshots {
		if err := inventory.ValidateCommittedSnapshot(record); err != nil {
			return nil, err
		}
		if record.State == inventory.CommittedSnapshotStatePending {
			records = append(records, inventory.CloneCommittedSnapshot(record))
		}
	}

	sort.Slice(records, func(lhs, rhs int) bool {
		return bytes.Compare(records[lhs].Snapshot.Hash, records[rhs].Snapshot.Hash) < 0
	})

	return records, nil
}

func (i *impl) MarkPosted(_ context.Context, hash []byte, postedAt time.Time) error {
	if postedAt.IsZero() {
		return pconfig.ErrInvalidArgs
	}

	key, err := inventorySnapshotKey(hash)
	if err != nil {
		return err
	}
	postedAt = postedAt.Round(0).UTC()

	defer i.lock.Unlock()
	i.lock.Lock()

	record, exists := i.inventorySnapshots[key]
	if !exists {
		return inventory.ErrCommittedSnapshotNotFound
	}
	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return err
	}
	if record.State == inventory.CommittedSnapshotStatePosted {
		if record.PostedAt.Equal(postedAt) {
			return nil
		}

		return inventory.ErrCommittedSnapshotConflict
	}

	shouldUpdateLatest := !i.hasLatestSnapshot
	if i.hasLatestSnapshot {
		latest, exists := i.inventorySnapshots[i.latestSnapshot]
		if !exists {
			return errors.New("memory: latest committed inventory snapshot does not exist")
		}
		if err := inventory.ValidateCommittedSnapshot(latest); err != nil {
			return err
		}
		if latest.State != inventory.CommittedSnapshotStatePosted {
			return errors.New("memory: latest committed inventory snapshot is not posted")
		}

		shouldUpdateLatest = !postedAt.Before(latest.PostedAt)
	}

	record, err = inventory.MarkCommittedSnapshotPosted(record, postedAt)
	if err != nil {
		return err
	}
	i.inventorySnapshots[key] = inventory.CloneCommittedSnapshot(record)
	if shouldUpdateLatest {
		i.latestSnapshot = key
		i.hasLatestSnapshot = true
	}

	return nil
}

func (i *impl) Get(_ context.Context, hash []byte) (inventory.CommittedSnapshot, error) {
	key, err := inventorySnapshotKey(hash)
	if err != nil {
		return inventory.CommittedSnapshot{}, err
	}

	defer i.lock.RUnlock()
	i.lock.RLock()

	record, exists := i.inventorySnapshots[key]
	if !exists || record.State != inventory.CommittedSnapshotStatePosted {
		return inventory.CommittedSnapshot{}, inventory.ErrCommittedSnapshotNotFound
	}
	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return inventory.CommittedSnapshot{}, err
	}

	return inventory.CloneCommittedSnapshot(record), nil
}

func (i *impl) Latest(_ context.Context) (inventory.CommittedSnapshot, error) {
	defer i.lock.RUnlock()
	i.lock.RLock()

	if !i.hasLatestSnapshot {
		return inventory.CommittedSnapshot{}, inventory.ErrCommittedSnapshotNotFound
	}

	record, exists := i.inventorySnapshots[i.latestSnapshot]
	if !exists {
		return inventory.CommittedSnapshot{}, errors.New("memory: latest committed inventory snapshot does not exist")
	}
	if err := inventory.ValidateCommittedSnapshot(record); err != nil {
		return inventory.CommittedSnapshot{}, err
	}
	if record.State != inventory.CommittedSnapshotStatePosted {
		return inventory.CommittedSnapshot{}, errors.New("memory: latest committed inventory snapshot is not posted")
	}

	return inventory.CloneCommittedSnapshot(record), nil
}

func inventorySnapshotKey(hash []byte) ([inventory.SnapshotHashSize]byte, error) {
	var key [inventory.SnapshotHashSize]byte

	if err := inventory.ValidateCommittedSnapshotHash(hash); err != nil {
		return key, err
	}

	copy(key[:], hash)

	return key, nil
}

func inventorySnapshotsEqual(lhs, rhs inventory.Snapshot) bool {
	return bytes.Equal(lhs.Payload, rhs.Payload) &&
		bytes.Equal(lhs.Hash, rhs.Hash) &&
		bytes.Equal(lhs.Signature, rhs.Signature) &&
		lhs.Provider == rhs.Provider
}
