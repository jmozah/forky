package forky

import (
	"github.com/dgraph-io/badger"
	"github.com/ethersphere/swarm/chunk"
)

type BadgerMetaStore struct {
	db *badger.DB
}

func NewBadgerMetaStore(path string) (s *BadgerMetaStore, err error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &BadgerMetaStore{db: db}, err
}

func (s *BadgerMetaStore) Get(addr chunk.Address) (m *Meta, err error) {
	if err := s.db.View(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(addr)
		if err != nil {
			return err
		}
		m = new(Meta)
		return item.Value(func(val []byte) error {
			return m.UnmarshalBinary(val)
		})
	}); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *BadgerMetaStore) Put(addr chunk.Address, m *Meta) (err error) {
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) (err error) {
		return txn.Set(addr, meta)
	})
}

func (s *BadgerMetaStore) Close() (err error) {
	return s.db.Close()
}
