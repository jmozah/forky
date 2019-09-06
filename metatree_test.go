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
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ethersphere/swarm/chunk"
)

func TestMetaTree(t *testing.T) {
	var mt metaTree

	addr := generateRandomAddress(32)
	meta := &Meta{
		Offset: 4094,
		Size:   42,
	}
	overwritten := mt.set(addr, meta)

	if overwritten {
		t.Error("should not be overwritten")
	}

	got := mt.get(addr)
	if !reflect.DeepEqual(got, meta) {
		t.Errorf("got meta %s, want %s", got, meta)
	}

	deleted := mt.delete(addr)
	if !deleted {
		t.Error("should be deleted")
	}

	got = mt.get(addr)
	if got != nil {
		t.Errorf("got meta %s, expected <nil>", got)
	}
}

func TestMetaTreeMultiAddress(t *testing.T) {
	var mt metaTree

	addrs := make([]chunk.Address, 7)
	addrs[0] = generateRandomAddress(64)
	addrs[1] = generateRandomAddress(32)
	addrs[2] = generateRandomAddress(17)
	addrs[3] = generateRandomAddress(32)
	addrs[4] = generateRandomAddress(64)
	addrs[5] = generateRandomAddress(35)
	addrs[6] = addrs[0][:32]

	metas := make([]*Meta, 7)
	metas[0] = &Meta{
		Offset: 4094,
		Size:   0,
	}
	metas[1] = &Meta{
		Offset: 4094 * 2,
		Size:   1,
	}
	metas[2] = &Meta{
		Offset: 0,
		Size:   2,
	}
	metas[3] = &Meta{
		Offset: 4094 * 10,
		Size:   3,
	}
	metas[4] = &Meta{
		Offset: 4094 * 100,
		Size:   4,
	}
	metas[5] = &Meta{
		Offset: 4094 * 25,
		Size:   5,
	}
	metas[6] = &Meta{
		Offset: 4094 * 32,
		Size:   6,
	}

	for i, addr := range addrs {
		overwritten := mt.set(addr, metas[i])
		if overwritten {
			t.Errorf("meta %v should not be overwritten", i)
		}
	}

	checkAddrs := func() {
		for i, addr := range addrs {
			want := metas[i]
			got := mt.get(addr)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got meta %v %s, want %s", i, got, want)
			}
		}
	}

	checkAddrs()

	deletedAddrs := make([]chunk.Address, 0)
	for i := 0; len(addrs) > 0; i++ {
		addr := addrs[0]
		deleted := mt.delete(addr)
		if !deleted {
			t.Errorf("meta %v should be deleted", i)
		}
		addrs = addrs[1:]
		metas = metas[1:]
		deletedAddrs = append(deletedAddrs, addr)

		checkAddrs()

		for i, addr := range deletedAddrs {
			got := mt.get(addr)
			if got != nil {
				t.Errorf("got meta for deleted address %v %s, expected <nil>", i, got)
			}
		}
	}
}

var resultBenchmarkMetaTree *Meta

func BenchmarkMapVsTree(b *testing.B) {
	const itemCount = 1000
	type item struct {
		addr chunk.Address
		meta *Meta
	}
	items := make([]item, itemCount)
	for i := 0; i < itemCount; i++ {
		items[i] = item{
			addr: generateRandomAddress(32),
			meta: &Meta{
				Offset: int64(i),
				Size:   uint16(i),
			},
		}
	}

	b.ResetTimer()

	b.Run("map", func(b *testing.B) {
		m := make(map[string]*Meta)
		b.Run("set", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					m[string(i.addr)] = i.meta
				}
			}
		})
		b.Run("get", func(b *testing.B) {
			var r *Meta
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					r = m[string(i.addr)]
				}
			}
			resultBenchmarkMetaTree = r
		})
		b.Run("delete", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					delete(m, string(i.addr))
				}
			}
		})
	})

	b.Run("tree", func(b *testing.B) {
		var mt metaTree
		b.Run("set", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					mt.set(i.addr, i.meta)
				}
			}
		})
		b.Run("get", func(b *testing.B) {
			var r *Meta
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					r = mt.get(i.addr)
				}
			}
			resultBenchmarkMetaTree = r
		})
		b.Run("delete", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					mt.delete(i.addr)
				}
			}
		})
	})
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateRandomAddress(size int) (addr chunk.Address) {
	addr = make([]byte, size)
	rand.Read(addr)
	return addr
}
