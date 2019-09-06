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

package mem

import (
	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
)

var _ forky.MetaStore = new(MetaStore)

type MetaStore struct {
	metaCache *forky.MetaCache
	freeCache *forky.OffsetCache
}

func NewMetaStore(shardCount uint8) (s *MetaStore) {
	return &MetaStore{
		metaCache: forky.NewMetaCache(),
		freeCache: forky.NewOffsetCache(shardCount),
	}
}

func (s *MetaStore) Get(addr chunk.Address) (m *forky.Meta, err error) {
	m = s.metaCache.Get(addr)
	if m == nil {
		return nil, chunk.ErrChunkNotFound
	}
	return m, nil
}

func (s *MetaStore) Has(addr chunk.Address) (yes bool, err error) {
	return s.metaCache.Get(addr) != nil, nil
}

func (s *MetaStore) Put(addr chunk.Address, shard uint8, reclaimed bool, m *forky.Meta) (err error) {
	s.metaCache.Set(addr, m)
	if reclaimed {
		s.freeCache.Delete(shard, m.Offset)
	}
	return nil
}

func (s *MetaStore) Delete(addr chunk.Address, shard uint8) (err error) {
	m := s.metaCache.Get(addr)
	if m == nil {
		return chunk.ErrChunkNotFound
	}
	s.metaCache.Delete(addr)
	s.freeCache.Set(shard, m.Offset)
	return nil
}

func (s *MetaStore) Free(shard uint8) (offset int64, err error) {
	return s.freeCache.Get(shard), nil
}

func (s *MetaStore) Close() (err error) {
	return nil
}
