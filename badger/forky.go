// Copyright 2019 The Swarm Authors
// This file is part of the Swarm library.
//
// The Swarm library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Swarm library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Swarm library. If not, see <http://www.gnu.org/licenses/>.

package badger

import (
	"github.com/dgraph-io/badger"
	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
)

var _ forky.MetaStore = new(MetaStore)

type MetaStore struct {
	db *badger.DB
}

func NewMetaStore(path string) (s *MetaStore, err error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &MetaStore{db: db}, err
}

func (s *MetaStore) Get(addr chunk.Address) (m *forky.Meta, err error) {
	if err := s.db.View(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(addr)
		if err != nil {
			return err
		}
		m = new(forky.Meta)
		return item.Value(func(val []byte) error {
			return m.UnmarshalBinary(val)
		})
	}); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *MetaStore) Set(addr chunk.Address, shard uint8, reclaimed bool, m *forky.Meta) (err error) {
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) (err error) {
		return txn.Set(addr, meta)
	})
}

func (s *MetaStore) Delete(addr chunk.Address, shard uint8) (err error) {
	return s.db.Update(func(txn *badger.Txn) (err error) {
		return txn.Delete(addr)
	})
}

func (s *MetaStore) Free(shard uint8) (offset int64, err error) {
	return
}

func (s *MetaStore) Close() (err error) {
	return s.db.Close()
}
