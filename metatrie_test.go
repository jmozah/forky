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

func TestMetaTrie(t *testing.T) {
	var mt metaTrie

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

	removed := mt.remove(addr)
	if !removed {
		t.Error("should be removed")
	}

	got = mt.get(addr)
	if got != nil {
		t.Errorf("got meta %s, expected <nil>", got)
	}
}

func TestMetaTrieMultiAddress(t *testing.T) {
	var mt metaTrie

	addrs := make([]chunk.Address, 11)
	// regular address
	addrs[0] = generateRandomAddress(32)
	// address for encrypted chunk
	addrs[1] = generateRandomAddress(64)
	// unusual address
	addrs[2] = generateRandomAddress(17)
	// regular address
	addrs[3] = generateRandomAddress(32)
	// regular address
	addrs[4] = append(generateRandomAddress(31), 0)
	// regular address with last only last byte different
	addrs[5] = append(append([]byte{}, addrs[3][:31]...), 1)
	// address for encrypted chunk
	addrs[6] = generateRandomAddress(64)
	// unusual long address
	addrs[7] = generateRandomAddress(1025)
	// unusual address
	addrs[8] = generateRandomAddress(35)
	// regular address, the same as encrypted one's first part
	addrs[9] = addrs[1][:32]
	// address for encrypted chunk that begins as one of regular addresses
	addrs[10] = append(append([]byte{}, addrs[0]...), generateRandomAddress(32)...)

	metas := make([]*Meta, 11)
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
	metas[7] = &Meta{
		Offset: 4094 * 11,
		Size:   7,
	}
	metas[8] = &Meta{
		Offset: 4094 * 4,
		Size:   8,
	}
	metas[9] = &Meta{
		Offset: 4094 * 72,
		Size:   9,
	}
	metas[10] = &Meta{
		Offset: 4094 * 26,
		Size:   10,
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

	removedAddrs := make([]chunk.Address, 0)
	for i := 0; len(addrs) > 0; i++ {
		addr := addrs[0]
		removed := mt.remove(addr)
		if !removed {
			t.Errorf("meta %v should be removed", i)
		}
		addrs = addrs[1:]
		metas = metas[1:]
		removedAddrs = append(removedAddrs, addr)

		checkAddrs()

		for i, addr := range removedAddrs {
			got := mt.get(addr)
			if got != nil {
				t.Errorf("got meta for removed address %v %s, expected <nil>", i, got)
			}
		}
	}
}

var resultBenchmarkMetaTrie *Meta

func BenchmarkMapVsTrie(b *testing.B) {
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
			resultBenchmarkMetaTrie = r
		})
		b.Run("delete", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					delete(m, string(i.addr))
				}
			}
		})
	})

	b.Run("trie", func(b *testing.B) {
		var mt metaTrie
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
			resultBenchmarkMetaTrie = r
		})
		b.Run("delete", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, i := range items {
					mt.remove(i.addr)
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
