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

package bolt

import (
	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
	bolt "go.etcd.io/bbolt"
)

var _ forky.MetaStore = new(MetaStore)

var (
	bucketNameMeta = []byte("Meta")
	bucketNameFree = []byte("Free")
)

type MetaStore struct {
	db *bolt.DB
}

func NewMetaStore(filename string) (s *MetaStore, err error) {
	db, err := bolt.Open(filename, 0666, &bolt.Options{
		NoSync: true,
	})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucketNameMeta)
		return err
	}); err != nil {
		return nil, err
	}
	return &MetaStore{db: db}, err
}

func (s *MetaStore) Get(addr chunk.Address) (m *forky.Meta, err error) {
	if err := s.db.View(func(tx *bolt.Tx) (err error) {
		data := tx.Bucket(bucketNameMeta).Get(addr)
		if data == nil {
			return chunk.ErrChunkNotFound
		}
		m = new(forky.Meta)
		if err := m.UnmarshalBinary(data); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *MetaStore) Put(addr chunk.Address, shard uint8, reclaimed bool, m *forky.Meta) (err error) {
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		return tx.Bucket(bucketNameMeta).Put(addr, meta)
	})
}

func (s *MetaStore) Free(shard uint8) (offset int64, err error) {
	return 0, nil
}

func (s *MetaStore) Delete(addr chunk.Address, shard uint8) (err error) {
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		return tx.Bucket(bucketNameMeta).Delete(addr)
	})
}

func (s *MetaStore) Close() (err error) {
	return s.db.Close()
}
