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
	"encoding/binary"

	"github.com/dgraph-io/badger/v2"
	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
)

var _ forky.MetaStore = new(MetaStore)

type MetaStore struct {
	db *badger.DB
}

func NewMetaStore(path string) (s *MetaStore, err error) {
	o := badger.DefaultOptions(path)
	o.Logger = nil
	db, err := badger.Open(o)
	if err != nil {
		return nil, err
	}
	return &MetaStore{db: db}, err
}

func (s *MetaStore) Get(addr chunk.Address) (m *forky.Meta, err error) {
	err = s.db.View(func(txn *badger.Txn) (err error) {
		m, err = getMeta(txn, chunkKey(addr))
		return err
	})
	return m, err
}

func (s *MetaStore) Set(addr chunk.Address, shard uint8, reclaimed bool, m *forky.Meta) (err error) {
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) (err error) {
		if reclaimed {
			err = txn.Delete(freeKey(shard, m.Offset))
			if err != nil {
				return err
			}
		}
		return txn.Set(chunkKey(addr), meta)
	})
}

func (s *MetaStore) FreeOffset(shard uint8) (offset int64, err error) {
	offset = -1
	err = s.db.View(func(txn *badger.Txn) (err error) {
		i := txn.NewIterator(badger.IteratorOptions{})
		defer i.Close()
		prefix := []byte{freePrefix, shard}
		i.Seek(prefix)
		if !i.ValidForPrefix(prefix) {
			return nil
		}
		item := i.Item()
		if item == nil {
			return nil
		}
		key := item.Key()
		if key == nil {
			return nil
		}
		offset = int64(binary.BigEndian.Uint64(key[1:9]))
		return err
	})
	return offset, err
}

func (s *MetaStore) Remove(addr chunk.Address, shard uint8) (err error) {
	return s.db.Update(func(txn *badger.Txn) (err error) {
		key := chunkKey(addr)
		m, err := getMeta(txn, key)
		if err != nil {
			return err
		}
		err = txn.Set(freeKey(shard, m.Offset), nil)
		if err != nil {
			return err
		}
		return txn.Delete(key)
	})
}

func (s *MetaStore) Count() (count int, err error) {
	err = s.db.View(func(txn *badger.Txn) (err error) {
		i := txn.NewIterator(badger.IteratorOptions{})
		defer i.Close()
		count++
		return nil
	})
	return count, err
}

func (s *MetaStore) Close() (err error) {
	return s.db.Close()
}

func getMeta(txn *badger.Txn, key []byte) (m *forky.Meta, err error) {
	item, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, chunk.ErrChunkNotFound
		}
		return nil, err
	}
	m = new(forky.Meta)
	return m, item.Value(func(val []byte) error {
		return m.UnmarshalBinary(val)
	})
}

const (
	chunkPrefix = 0
	freePrefix  = 1
)

func chunkKey(addr chunk.Address) (key []byte) {
	return append([]byte{chunkPrefix}, addr...)
}

func freeKey(shard uint8, offset int64) (key []byte) {
	key = make([]byte, 10)
	key[0] = freePrefix
	key[1] = shard
	binary.BigEndian.PutUint64(key[2:10], uint64(offset))
	return key
}
