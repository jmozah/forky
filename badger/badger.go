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
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
)

var _ forky.Interface = new(BadgerStore)

type BadgerStore struct {
	db *badger.DB
}

func NewBadgerStore(path string) (s *BadgerStore, err error) {
	o := badger.DefaultOptions(path).WithSyncWrites(false).WithCompression(options.None)
	o.Logger = nil
	db, err := badger.Open(o)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{
		db: db,
	}, nil
}

func (s *BadgerStore) Get(addr chunk.Address) (c chunk.Chunk, err error) {
	err = s.db.View(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(addr)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return  chunk.ErrChunkNotFound
			}
		}
		return item.Value(func(val []byte) error {
			c = chunk.NewChunk(addr, val)
			return nil
		})
	})
	return c, err
}

func (s *BadgerStore) Has(addr chunk.Address) (yes bool, err error) {
	yes = false
	err = s.db.View(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(addr)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return  chunk.ErrChunkNotFound
			}
		}
		return item.Value(func(val []byte) error {
			yes = true
			return nil
		})
	})
	return yes, err
}

func (s *BadgerStore) Put(ch chunk.Chunk) (err error) {
	return s.db.Update(func(txn *badger.Txn) (err error) {
		err = txn.Set(ch.Address(), ch.Data())
		return err
	})
}

func (s *BadgerStore) Delete(addr chunk.Address) (err error) {
	return s.db.Update(func(txn *badger.Txn) (err error) {
		return txn.Delete(addr)
	})
}

func (s *BadgerStore) Count() (count int, err error) {
	err = s.db.View(func(txn *badger.Txn) (err error) {
		i := txn.NewIterator(badger.IteratorOptions{})
		defer i.Close()
		for i.Rewind(); i.Valid(); i.Next() {
			item := i.Item()
			k := item.Key()
			if len(k) < 1 {
				continue
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) == 0 {
				continue
			}
			count++
		}
		return nil
	})
	return count, err
}

func (s *BadgerStore) Iterate(fn func(chunk.Chunk) (stop bool, err error)) (err error) {
	return s.db.View(func(txn *badger.Txn) (err error) {
		i := txn.NewIterator(badger.IteratorOptions{})
		defer i.Close()
		for i.Rewind(); i.Valid(); i.Next() {
			item := i.Item()
			k := item.Key()
			if len(k) < 1 {
				continue
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(v) == 0 {
				continue
			}
			m := new(forky.Meta)
			if err := m.UnmarshalBinary(v); err != nil {
				return err
			}
			stop, err := fn(chunk.NewChunk(k,v))
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		return nil
	})
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}
