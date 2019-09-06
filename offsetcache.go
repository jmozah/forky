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

package forky

import "sync"

type OffsetCache struct {
	m  map[uint8]map[int64]struct{}
	mu sync.RWMutex
}

func NewOffsetCache(shardCount uint8) (c *OffsetCache) {
	m := make(map[uint8]map[int64]struct{})
	for i := uint8(0); i < shardCount; i++ {
		m[i] = make(map[int64]struct{})
	}
	return &OffsetCache{
		m: m,
	}
}

func (c *OffsetCache) Get(shard uint8) (offset int64) {
	c.mu.RLock()
	for o := range c.m[shard] {
		c.mu.RUnlock()
		return o
	}
	c.mu.RUnlock()
	return -1
}

func (c *OffsetCache) Set(shard uint8, offset int64) {
	c.mu.Lock()
	c.m[shard][offset] = struct{}{}
	c.mu.Unlock()
}

func (c *OffsetCache) Delete(shard uint8, offset int64) {
	c.mu.Lock()
	delete(c.m[shard], offset)
	c.mu.Unlock()
}
