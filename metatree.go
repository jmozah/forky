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
	"github.com/ethersphere/swarm/chunk"
)

type metaTree struct {
	byte     byte
	value    *Meta
	branches []*metaTree
}

func (t *metaTree) get(addr chunk.Address) (m *Meta) {
	v := addr[0]
	for _, b := range t.branches {
		if b.byte == v {
			if len(addr) == 1 {
				return b.value
			}
			return b.get(addr[1:])
		}
	}
	return nil
}

func (t *metaTree) set(addr chunk.Address, m *Meta) (overwritten bool) {
	find := func(v byte, branches []*metaTree) (i int) {
		for i, b := range branches {
			if b.byte == v {
				return i
			}
		}
		return -1
	}
	x := t
	overwritten = true
	for _, v := range addr {
		i := find(v, x.branches)
		if i < 0 {
			i = len(x.branches)
			x.branches = append(x.branches, &metaTree{
				byte: v,
			})
			overwritten = false
		}
		x = x.branches[i]
	}
	x.value = m
	if overwritten {
		overwritten = x.branches == nil
	}
	return overwritten
}

func (t *metaTree) delete(addr chunk.Address) (deleted bool) {
	v := addr[0]
	for _, b := range t.branches {
		if b.byte == v {
			if len(addr) == 2 {
				v := addr[1]
				for i, x := range b.branches {
					if x.byte == v {
						b.branches = append(b.branches[:i], b.branches[i+1:]...)
						return true
					}
				}
			}
			return b.delete(addr[1:])
		}
	}
	return false
}
