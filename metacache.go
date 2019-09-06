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

import (
	"sync"

	"github.com/ethersphere/swarm/chunk"
)

type metaCache struct {
	m  metaTree
	mu sync.RWMutex
}

func newMetaCache() (c *metaCache) {
	return new(metaCache)
}

func (c *metaCache) get(addr chunk.Address) (m *Meta) {
	c.mu.RLock()
	m = c.m.get(addr)
	c.mu.RUnlock()
	return m
}

func (c *metaCache) set(addr chunk.Address, m *Meta) {
	c.mu.Lock()
	c.m.set(addr, m)
	c.mu.Unlock()
}

func (c *metaCache) delete(addr chunk.Address) {
	c.mu.Lock()
	c.m.delete(addr)
	c.mu.Unlock()
}
