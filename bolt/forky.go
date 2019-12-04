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
	"encoding/binary"

	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
	bolt "go.etcd.io/bbolt"
)

var _ forky.MetaStore = new(MetaStore)

var (
	bucketNameChunkMeta   = []byte("ChunkMeta")
	bucketNameFreeOffsets = []byte("FreeOffsets")
)

type MetaStore struct {
	db *bolt.DB
}

func NewMetaStore(filename string, noSync bool) (s *MetaStore, err error) {
	db, err := bolt.Open(filename, 0666, &bolt.Options{
		NoSync: noSync,
	})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucketNameChunkMeta)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketNameFreeOffsets)
		return err
	}); err != nil {
		return nil, err
	}
	return &MetaStore{db: db}, err
}

func (s *MetaStore) Get(addr chunk.Address) (m *forky.Meta, err error) {
	err = s.db.View(func(tx *bolt.Tx) (err error) {
		m, err = getMeta(tx.Bucket(bucketNameChunkMeta), addr)
		return err
	})
	return m, err
}

func (s *MetaStore) Set(addr chunk.Address, shard uint8, reclaimed bool, m *forky.Meta) (err error) {
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		if reclaimed {
			err = tx.Bucket(bucketNameFreeOffsets).Delete(freeKey(shard, m.Offset))
			if err != nil {
				return err
			}
		}
		return tx.Bucket(bucketNameChunkMeta).Put(addr, meta)
	})
}

func (s *MetaStore) FreeOffset(shard uint8) (offset int64, err error) {
	offset = -1
	err = s.db.View(func(tx *bolt.Tx) (err error) {
		c := tx.Bucket(bucketNameFreeOffsets).Cursor()
		key, _ := c.Seek([]byte{shard})
		if key == nil || key[0] != shard {
			return nil
		}
		offset = int64(binary.BigEndian.Uint64(key[1:9]))
		return err
	})
	return offset, err
}

func (s *MetaStore) Remove(addr chunk.Address, shard uint8) (err error) {
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bucketNameChunkMeta)
		m, err := getMeta(b, addr)
		if err != nil {
			return err
		}
		err = tx.Bucket(bucketNameFreeOffsets).Put(freeKey(shard, m.Offset), nil)
		if err != nil {
			return err
		}
		return b.Delete(addr)
	})
}

func (s *MetaStore) Count() (count int, err error) {
	err = s.db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bucketNameChunkMeta)
		if b == nil {
			return nil
		}
		count = b.Stats().KeyN
		return nil
	})
	return count, err
}

func (s *MetaStore) Iterate(fn func(chunk.Address, *forky.Meta) (stop bool, err error)) (err error) {
	return s.db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket(bucketNameChunkMeta)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) (err error) {
			m := new(forky.Meta)
			if err := m.UnmarshalBinary(v); err != nil {
				return err
			}
			stop, err := fn(chunk.Address(k), m)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
			return nil
		})
	})
}

func (s *MetaStore) Close() (err error) {
	return s.db.Close()
}

func getMeta(b *bolt.Bucket, addr chunk.Address) (m *forky.Meta, err error) {
	data := b.Get(addr)
	if data == nil {
		return nil, chunk.ErrChunkNotFound
	}
	m = new(forky.Meta)
	if err := m.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return m, nil
}

func freeKey(shard uint8, offset int64) (key []byte) {
	key = make([]byte, 9)
	key[0] = shard
	binary.BigEndian.PutUint64(key[1:9], uint64(offset))
	return key
}
