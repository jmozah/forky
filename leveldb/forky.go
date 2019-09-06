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

package leveldb

import (
	"encoding/binary"

	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var _ forky.MetaStore = new(MetaStore)

type MetaStore struct {
	db        *leveldb.DB
	metaCache *forky.MetaCache
	freeCache *forky.OffsetCache
}

func NewMetaStore(filename string, metaCache *forky.MetaCache, freeCache *forky.OffsetCache) (s *MetaStore, err error) {
	db, err := leveldb.OpenFile(filename, &opt.Options{})
	if err != nil {
		return nil, err
	}
	return &MetaStore{
		db:        db,
		metaCache: metaCache,
		freeCache: freeCache,
	}, err
}

func (s *MetaStore) Get(addr chunk.Address) (m *forky.Meta, err error) {
	if s.metaCache != nil {
		m = s.metaCache.Get(addr)
	}
	if m != nil {
		return m, nil
	}
	data, err := s.db.Get(chunkKey(addr), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, chunk.ErrChunkNotFound
		}
		return nil, err
	}
	m = new(forky.Meta)
	if err := m.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	if s.metaCache != nil {
		s.metaCache.Set(addr, m)
	}
	return m, nil
}

func (s *MetaStore) Has(addr chunk.Address) (yes bool, err error) {
	_, err = s.Get(addr)
	if err != nil {
		if err == chunk.ErrChunkNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *MetaStore) Put(addr chunk.Address, shard uint8, reclaimed bool, m *forky.Meta) (err error) {
	batch := new(leveldb.Batch)
	if reclaimed {
		batch.Delete(freeKey(shard, m.Offset))
		if s.freeCache != nil {
			s.freeCache.Delete(shard, m.Offset)
		}
	}
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	batch.Put(chunkKey(addr), meta)
	if s.metaCache != nil {
		s.metaCache.Set(addr, m)
	}
	return s.db.Write(batch, nil)
}

func (s *MetaStore) Free(shard uint8) (offset int64, err error) {
	if s.freeCache != nil {
		if o := s.freeCache.Get(shard); o >= 0 {
			return o, nil
		}
	}

	i := s.db.NewIterator(nil, nil)
	defer i.Release()

	i.Seek([]byte{freePrefix, shard})
	key := i.Key()
	if key == nil || key[0] != freePrefix || key[1] != shard {
		return -1, nil
	}
	offset = int64(binary.BigEndian.Uint64(key[2:10]))
	if s.freeCache != nil {
		s.freeCache.Set(shard, offset)
	}
	return offset, nil
}

func (s *MetaStore) Delete(addr chunk.Address, shard uint8) (err error) {
	m, err := s.Get(addr)
	if err != nil {
		return err
	}
	batch := new(leveldb.Batch)
	batch.Put(freeKey(shard, m.Offset), nil)
	if s.metaCache != nil {
		s.metaCache.Delete(addr)
	}
	if s.freeCache != nil {
		s.freeCache.Set(shard, m.Offset)
	}
	batch.Delete(chunkKey(addr))
	return s.db.Write(batch, nil)
}

func (s *MetaStore) Close() (err error) {
	return s.db.Close()
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
