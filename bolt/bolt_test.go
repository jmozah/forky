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

package bolt_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethersphere/swarm/chunk"
	"github.com/janos/forky/test"
	bolt "go.etcd.io/bbolt"
)

func BenchmarkBoltDBPut(b *testing.B) {
	count := 1000

	var bucketNameChunkMeta = []byte("ChunkMeta")

	db, clean := newBoltDB(b)
	defer clean()

	if err := db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucketNameChunkMeta)
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for i := 0; i < count; i++ {
			if err := db.Update(func(tx *bolt.Tx) (err error) {
				return tx.Bucket(bucketNameChunkMeta).Put([]byte{uint8(i)}, nil)
			}); err != nil {
				b.Fatal(err)
			}
		}
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
			chunks[i] = test.GenerateTestRandomChunk()
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

func newBoltDB(t testing.TB) (db *bolt.DB, clean func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "swarm-forky-")
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
