package forky_test

import (
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky"
	bolt "go.etcd.io/bbolt"
)

var defaultChunkCounts = []int{1, 10, 100, 1000, 10000, 100000, 1000000}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestForky(t *testing.T) {
	testStore(t, defaultChunkCounts, 0, false, newForkyDB)
}

func TestForkyWithDelete(t *testing.T) {
	testCases := []struct {
		name        string
		deleteSplit int
	}{
		{
			name:        "nothing deleted",
			deleteSplit: 0,
		},
		{
			name:        "all deleted",
			deleteSplit: 1,
		},
		{
			name:        "half deleted",
			deleteSplit: 2,
		},
		{
			name:        "third deleted",
			deleteSplit: 3,
		},
		{
			name:        "quarter deleted",
			deleteSplit: 4,
		},
		{
			name:        "fifth deleted",
			deleteSplit: 5,
		},
		{
			name:        "tenth deleted",
			deleteSplit: 10,
		},
		{
			name:        "hundredth deleted",
			deleteSplit: 100,
		},
		{
			name:        "thousandth deleted",
			deleteSplit: 1000,
		},
	}
	t.Run("prepopulated", func(t *testing.T) {
		testStore(t, []int{1000000}, 0, true, newForkyDB)
	})
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testStore(t, []int{1000000}, tc.deleteSplit, false, newForkyDB)
		})
	}
}

func testStore(t *testing.T, counts []int, deleteSplit int, prepopulate bool, newStore func(t *testing.T) (forky.Store, func())) {
	t.Helper()

	var max int
	for _, n := range counts {
		if n > max {
			max = n
		}
	}

	chunks := make([]chunk.Chunk, max)
	for i := 0; i < max; i++ {
		ch := generateTestRandomChunk()

		chunks[i] = ch
	}

	for _, count := range counts {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			db, clean := newStore(t)
			defer clean()

			if prepopulate {
				t.Run("prepopulate", func(t *testing.T) {
					sem := make(chan struct{}, 8)
					var wg sync.WaitGroup
					wg.Add(count)
					for _, ch := range chunks[:count] {
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
					wg.Add(count)
					for _, ch := range chunks[:count] {
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

			rand.Shuffle(count, func(i, j int) {
				chunks[i], chunks[j] = chunks[j], chunks[i]
			})

			t.Run("write", func(t *testing.T) {
				sem := make(chan struct{}, 8)
				var wg sync.WaitGroup
				wg.Add(count)
				for i, ch := range chunks[:count] {
					sem <- struct{}{}
					go func(i int, ch chunk.Chunk) {
						defer func() {
							<-sem
							wg.Done()
						}()
						if err := db.Put(ch); err != nil {
							panic(err)
						}
						if deleteSplit > 0 && i%deleteSplit == 0 {
							if err := db.Delete(ch.Address()); err != nil {
								panic(err)
							}
						}
					}(i, ch)
				}
				wg.Wait()
			})

			rand.Shuffle(count, func(i, j int) {
				chunks[i], chunks[j] = chunks[j], chunks[i]
			})

			t.Run("read", func(t *testing.T) {
				for i, ch := range chunks[:count] {
					if deleteSplit > 0 && i%deleteSplit == 0 {
						_, err := db.Get(ch.Address())
						if err != chunk.ErrChunkNotFound {
							t.Errorf("got error %v, want %v", err, chunk.ErrChunkNotFound)
						}
						continue
					}
					got, err := db.Get(ch.Address())
					if err != nil {
						t.Fatalf("chunk %v %s: %v", i, ch.Address().Hex(), err)
					}

					if !bytes.Equal(got.Address(), ch.Address()) {
						t.Fatalf("got chunk %v address %x, want %x", i, got.Address(), ch.Address())
					}
					if !bytes.Equal(got.Data(), ch.Data()) {
						t.Fatalf("got chunk %v data %x, want %x", i, got.Data(), ch.Data())
					}
				}
			})
		})
	}
}

func TestForky2(t *testing.T) {
	testStore2(t, newForkyDB)
}

func testStore2(t *testing.T, newStore func(t *testing.T) (forky.Store, func())) {
	t.Helper()

	totalChunks := 1000000
	iterationCount := 10000

	chunks := make([]chunk.Chunk, totalChunks)
	for i := 0; i < totalChunks; i++ {
		ch := generateTestRandomChunk()

		chunks[i] = ch
	}

	db, clean := newStore(t)
	defer clean()

	for i := 1; i <= totalChunks/iterationCount; i++ {
		t.Run(strconv.Itoa(i*iterationCount), func(t *testing.T) {

			t.Run("write", func(t *testing.T) {
				sem := make(chan struct{}, 8)
				var wg sync.WaitGroup
				wg.Add(iterationCount)
				for _, ch := range chunks[(i-1)*iterationCount : i*iterationCount] {
					sem <- struct{}{}
					go func(ch chunk.Chunk) {
						defer func() {
							<-sem
							wg.Done()
						}()
						if err := db.Put(ch); err != nil {
							t.Fatal(err)
						}
					}(ch)
				}
				wg.Wait()
			})

			t.Run("read", func(t *testing.T) {
				for i, ch := range chunks[(i-1)*iterationCount : i*iterationCount] {
					got, err := db.Get(ch.Address())
					if err != nil {
						t.Fatalf("chunk %v %s: %v", i, ch.Address().Hex(), err)
					}

					if !bytes.Equal(got.Address(), ch.Address()) {
						t.Fatalf("got chunk %v address %x, want %x", i, got.Address(), ch.Address())
					}
					if !bytes.Equal(got.Data(), ch.Data()) {
						t.Fatalf("got chunk %v data %x, want %x", i, got.Data(), ch.Data())
					}
				}
			})
		})
	}
}

func newForkyDB(t *testing.T) (db forky.Store, clean func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "swarm-forky")
	if err != nil {
		t.Fatal(err)
	}

	var metaStore forky.MetaStore

	// metaStore = forky.NewMapMetaStore()
	// metaStore, err = forky.NewBoltMetaStore(filepath.Join(path, "meta.db"))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	metaStore, err = forky.NewLevelDBMetaStore(filepath.Join(path, "meta"), forky.ShardCount)
	if err != nil {
		t.Fatal(err)
	}

	// metaStore, err = forky.NewBadgerMetaStore(filepath.Join(path, "meta"))
	// if err != nil {
	// 	t.Fatal(err)
	// }

	db, err = forky.NewDB(path, metaStore)
	if err != nil {
		os.RemoveAll(path)
		t.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(path)
	}
}

func newBoltDB(t *testing.T) (db *bolt.DB, clean func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "swarm-forky")
	if err != nil {
		t.Fatal(err)
	}

	db, err = bolt.Open(filepath.Join(path, "test.db"), 0666, nil)
	if err != nil {
		os.RemoveAll(path)
		t.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(path)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateTestRandomChunk() chunk.Chunk {
	data := make([]byte, chunk.DefaultSize)
	rand.Read(data)
	key := make([]byte, 32)
	rand.Read(key)
	return chunk.NewChunk(key, data)
}

func TestBoltDBPut(t *testing.T) {
	var bucketNameChunkMeta = []byte("ChunkMeta")
	for _, count := range []int{
		1, 10, 100, 1000,
	} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			db, clean := newBoltDB(t)
			defer clean()

			if err := db.Update(func(tx *bolt.Tx) (err error) {
				_, err = tx.CreateBucketIfNotExists(bucketNameChunkMeta)
				return err
			}); err != nil {
				t.Fatal(err)
			}

			chunks := make([]chunk.Chunk, count)
			for i := 0; i < count; i++ {
				ch := generateTestRandomChunk()

				if err := db.Update(func(tx *bolt.Tx) (err error) {
					return tx.Bucket(bucketNameChunkMeta).Put([]byte{uint8(i)}, nil)
				}); err != nil {
					t.Fatal(err)
				}

				chunks[i] = ch
			}
		})
	}
}

func BenchmarkBoltDB(b *testing.B) {
	var bucketNameChunkMeta = []byte("ChunkMeta")
	const count = 1000

	test := func(b *testing.B, sync bool) {
		path, err := ioutil.TempDir("", "swarm-bolt")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(path)

		db, err := bolt.Open(filepath.Join(path, "test.db"), 0666, &bolt.Options{
			NoSync: !sync,
		})
		if err != nil {
			b.Fatal(err)
		}

		if err := db.Update(func(tx *bolt.Tx) (err error) {
			_, err = tx.CreateBucketIfNotExists(bucketNameChunkMeta)
			return err
		}); err != nil {
			b.Fatal(err)
		}

		chunks := make([]chunk.Chunk, count)
		for i := 0; i < count; i++ {
			chunks[i] = generateTestRandomChunk()
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for _, ch := range chunks {
				if err := db.Update(func(tx *bolt.Tx) (err error) {
					return tx.Bucket(bucketNameChunkMeta).Put(ch.Address(), nil)
				}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	b.Run("sync", func(b *testing.B) {
		test(b, true)
	})
	b.Run("nosync", func(b *testing.B) {
		test(b, false)
	})
}

var result string

func BenchmarkStringandHEX(b *testing.B) {
	v := make([]byte, 32)
	b.Run("string", func(b *testing.B) {
		var r string
		for i := 0; i < b.N; i++ {
			r = string(v)
		}
		result = r
	})
	b.Run("hex", func(b *testing.B) {
		var r string
		for i := 0; i < b.N; i++ {
			r = hex.EncodeToString(v)
		}
		result = r
	})
}
