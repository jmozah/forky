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

package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
)

func StoreSuite(t *testing.T, chunkCounts []int, newStoreFunc func(t *testing.T) (forky.Interface, func())) {
	if *chunksFlag > 0 {
		chunkCounts = []int{*chunksFlag}
	}
	for _, chunkCount := range chunkCounts {
		t.Run(strconv.Itoa(chunkCount)+"-chunks", func(t *testing.T) {
			t.Run("empty", func(t *testing.T) {
				TestStore(t, &TestStoreOptions{
					ChunkCount:   chunkCount,
					NewStoreFunc: newStoreFunc,
				})
			})

			t.Run("cleaned", func(t *testing.T) {
				TestStore(t, &TestStoreOptions{
					ChunkCount:   chunkCount,
					NewStoreFunc: newStoreFunc,
					Cleaned:      true,
				})
			})

			for _, tc := range []struct {
				name        string
				deleteSplit int
			}{
				{
					name:        "delete-all",
					deleteSplit: 1,
				},
				{
					name:        "delete-half",
					deleteSplit: 2,
				},
				{
					name:        "delete-fifth",
					deleteSplit: 5,
				},
				{
					name:        "delete-tenth",
					deleteSplit: 10,
				},
				{
					name:        "delete-percent",
					deleteSplit: 100,
				},
				{
					name:        "delete-permill",
					deleteSplit: 1000,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					TestStore(t, &TestStoreOptions{
						ChunkCount:   chunkCount,
						DeleteSplit:  tc.deleteSplit,
						NewStoreFunc: newStoreFunc,
					})
				})
			}
		})
	}
}

type TestStoreOptions struct {
	ChunkCount   int
	DeleteSplit  int
	Cleaned      bool
	NewStoreFunc func(t *testing.T) (forky.Interface, func())
}

func TestStore(t *testing.T, o *TestStoreOptions) {
	db, clean := o.NewStoreFunc(t)
	defer clean()

	chunks := getChunks(o.ChunkCount)

	if o.Cleaned {
		t.Run("clean", func(t *testing.T) {
			sem := make(chan struct{}, *concurrencyFlag)
			var wg sync.WaitGroup

			wg.Add(o.ChunkCount)
			for _, ch := range chunks {
				sem <- struct{}{}

				go func(ch chunk.Chunk) {
					defer func() {
						<-sem
						wg.Done()
					}()

					if err := db.Put(ch); err != nil {
						panic(err)
					}
				}(ch)
			}
			wg.Wait()

			wg = sync.WaitGroup{}

			wg.Add(o.ChunkCount)
			for _, ch := range chunks {
				sem <- struct{}{}

				go func(ch chunk.Chunk) {
					defer func() {
						<-sem
						wg.Done()
					}()

					if err := db.Delete(ch.Address()); err != nil {
						panic(err)
					}
				}(ch)
			}
			wg.Wait()
		})
	}

	rand.Shuffle(o.ChunkCount, func(i, j int) {
		chunks[i], chunks[j] = chunks[j], chunks[i]
	})

	var deletedChunks sync.Map

	t.Run("write", func(t *testing.T) {
		sem := make(chan struct{}, *concurrencyFlag)
		var wg sync.WaitGroup

		wg.Add(o.ChunkCount)
		for i, ch := range chunks {
			sem <- struct{}{}

			go func(i int, ch chunk.Chunk) {
				defer func() {
					<-sem
					wg.Done()
				}()

				if err := db.Put(ch); err != nil {
					panic(err)
				}
				if o.DeleteSplit > 0 && i%o.DeleteSplit == 0 {
					if err := db.Delete(ch.Address()); err != nil {
						panic(err)
					}
					deletedChunks.Store(string(ch.Address()), nil)
				}
			}(i, ch)
		}
		wg.Wait()
	})

	rand.Shuffle(o.ChunkCount, func(i, j int) {
		chunks[i], chunks[j] = chunks[j], chunks[i]
	})

	t.Run("read", func(t *testing.T) {
		sem := make(chan struct{}, *concurrencyFlag)
		var wg sync.WaitGroup

		wg.Add(o.ChunkCount)
		for i, ch := range chunks {
			sem <- struct{}{}

			go func(i int, ch chunk.Chunk) {
				defer func() {
					<-sem
					wg.Done()
				}()

				got, err := db.Get(ch.Address())

				if _, ok := deletedChunks.Load(string(ch.Address())); ok {
					if err != chunk.ErrChunkNotFound {
						panic(fmt.Errorf("got error %v, want %v", err, chunk.ErrChunkNotFound))
					}
				} else {
					if err != nil {
						panic(fmt.Errorf("chunk %v %s: %v", i, ch.Address().Hex(), err))
					}
					if !bytes.Equal(got.Address(), ch.Address()) {
						panic(fmt.Errorf("got chunk %v address %x, want %x", i, got.Address(), ch.Address()))
					}
					if !bytes.Equal(got.Data(), ch.Data()) {
						panic(fmt.Errorf("got chunk %v data %x, want %x", i, got.Data(), ch.Data()))
					}
				}
			}(i, ch)
		}
		wg.Wait()
	})
}

// func TestForky2(t *testing.T) {
// 	testStore2(t, NewStore)
// }

// func testStore2(t *testing.T, newStore func(t *testing.T) (forky.Store, func())) {
// 	t.Helper()

// 	totalChunks := 1000000
// 	iterationCount := 10000

// 	chunks := make([]chunk.Chunk, totalChunks)
// 	for i := 0; i < totalChunks; i++ {
// 		ch := GenerateTestRandomChunk()

// 		chunks[i] = ch
// 	}

// 	db, clean := newStore(t)
// 	defer clean()

// 	for i := 1; i <= totalChunks/iterationCount; i++ {
// 		t.Run(strconv.Itoa(i*iterationCount), func(t *testing.T) {

// 			t.Run("write", func(t *testing.T) {
// 				sem := make(chan struct{}, 8)
// 				var wg sync.WaitGroup
// 				wg.Add(iterationCount)
// 				for _, ch := range chunks[(i-1)*iterationCount : i*iterationCount] {
// 					sem <- struct{}{}
// 					go func(ch chunk.Chunk) {
// 						defer func() {
// 							<-sem
// 							wg.Done()
// 						}()
// 						if err := db.Put(ch); err != nil {
// 							t.Fatal(err)
// 						}
// 					}(ch)
// 				}
// 				wg.Wait()
// 			})

// 			t.Run("read", func(t *testing.T) {
// 				for i, ch := range chunks[(i-1)*iterationCount : i*iterationCount] {
// 					got, err := db.Get(ch.Address())
// 					if err != nil {
// 						t.Fatalf("chunk %v %s: %v", i, ch.Address().Hex(), err)
// 					}

// 					if !bytes.Equal(got.Address(), ch.Address()) {
// 						t.Fatalf("got chunk %v address %x, want %x", i, got.Address(), ch.Address())
// 					}
// 					if !bytes.Equal(got.Data(), ch.Data()) {
// 						t.Fatalf("got chunk %v data %x, want %x", i, got.Data(), ch.Data())
// 					}
// 				}
// 			})
// 		})
// 	}
// }

func NewForkyStore(t *testing.T, path string, metaStore forky.MetaStore) (s *forky.Store, clean func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "swarm-forky")
	if err != nil {
		t.Fatal(err)
	}

	s, err = forky.NewStore(path, metaStore, *noCacheFlag)
	if err != nil {
		os.RemoveAll(path)
		t.Fatal(err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(path)
	}
}

var chunkCache []chunk.Chunk

func getChunks(count int) []chunk.Chunk {
	l := len(chunkCache)
	if l == 0 {
		chunkCache = make([]chunk.Chunk, count)
		for i := 0; i < count; i++ {
			chunkCache[i] = GenerateTestRandomChunk()
		}
		return chunkCache
	}
	if l < count {
		for i := 0; i < count-l; i++ {
			chunkCache = append(chunkCache, GenerateTestRandomChunk())
		}
		return chunkCache
	}
	return chunkCache[:count]
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GenerateTestRandomChunk() chunk.Chunk {
	data := make([]byte, chunk.DefaultSize)
	rand.Read(data)
	key := make([]byte, 32)
	rand.Read(key)
	return chunk.NewChunk(key, data)
}
