package forky_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/janos/forky"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestLevelDB(t *testing.T) {
	testStore(t, defaultChunkCounts, 0, false, newLevelDB)
}

func TestLevelDB2(t *testing.T) {
	testStore2(t, newLevelDB)
}

func newLevelDB(t *testing.T) (db forky.Store, clean func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "swarm-shed")
	if err != nil {
		t.Fatal(err)
	}

	db, err = NewLevelDBStore(path)
	if err != nil {
		os.RemoveAll(path)
		t.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(path)
	}
}

var _ forky.Store = new(LevelDBStore)

type LevelDBStore struct {
	db *leveldb.DB
}

func NewLevelDBStore(path string) (s *LevelDBStore, err error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: 128,
	})
	if err != nil {
		return nil, err
	}
	return &LevelDBStore{
		db: db,
	}, nil
}

func (s *LevelDBStore) Put(ch storage.Chunk) (err error) {
	return s.db.Put(ch.Address(), ch.Data(), nil)
}

func (s *LevelDBStore) Get(addr storage.Address) (c storage.Chunk, err error) {
	data, err := s.db.Get(addr, nil)
	if err != nil {
		return nil, err
	}
	return chunk.NewChunk(addr, data), nil
}

func (s *LevelDBStore) Delete(addr storage.Address) (err error) {
	return s.db.Delete(addr, nil)
}

// Close closes the underlying database.
func (s *LevelDBStore) Close() error {
	return s.db.Close()
}
