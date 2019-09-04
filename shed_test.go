package forky_test

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/ethersphere/swarm/shed"
	"github.com/ethersphere/swarm/storage"
	"github.com/janos/forky"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestShed(t *testing.T) {
	testStore(t, defaultChunkCounts, 0, false, newShedDB)
}

func newShedDB(t *testing.T) (db forky.Store, clean func()) {
	t.Helper()

	path, err := ioutil.TempDir("", "swarm-shed")
	if err != nil {
		t.Fatal(err)
	}

	db, err = NewShedStore(path)
	if err != nil {
		os.RemoveAll(path)
		t.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(path)
	}
}

var _ forky.Store = new(ShedStore)

type ShedStore struct {
	db *shed.DB

	retrievalIndex shed.Index
	accessIndex    shed.Index
	gcIndex        shed.Index
	gcSize         shed.Uint64Field
}

func NewShedStore(path string) (s *ShedStore, err error) {
	db, err := shed.NewDB(path, "")
	if err != nil {
		return nil, err
	}
	s = &ShedStore{
		db: db,
	}
	s.retrievalIndex, err = db.NewIndex("Address->StoreTimestamp|Data", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			return fields.Address, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(fields.StoreTimestamp))
			value = append(b, fields.Data...)
			return value, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.StoreTimestamp = int64(binary.BigEndian.Uint64(value[:8]))
			e.Data = value[8:]
			return e, nil
		},
	})
	if err != nil {
		return nil, err
	}
	s.accessIndex, err = db.NewIndex("Address->AccessTimestamp", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			return fields.Address, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(fields.AccessTimestamp))
			return b, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.AccessTimestamp = int64(binary.BigEndian.Uint64(value))
			return e, nil
		},
	})
	if err != nil {
		return nil, err
	}
	s.gcIndex, err = db.NewIndex("AccessTimestamp|StoredTimestamp|Address->nil", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			b := make([]byte, 16, 16+len(fields.Address))
			binary.BigEndian.PutUint64(b[:8], uint64(fields.AccessTimestamp))
			binary.BigEndian.PutUint64(b[8:16], uint64(fields.StoreTimestamp))
			key = append(b, fields.Address...)
			return key, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.AccessTimestamp = int64(binary.BigEndian.Uint64(key[:8]))
			e.StoreTimestamp = int64(binary.BigEndian.Uint64(key[8:16]))
			e.Address = key[16:]
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			return nil, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			return e, nil
		},
	})
	if err != nil {
		return nil, err
	}
	s.gcSize, err = db.NewUint64Field("gc-size")
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ShedStore) Put(ch storage.Chunk) (err error) {
	return s.retrievalIndex.Put(shed.Item{
		Address:        ch.Address(),
		Data:           ch.Data(),
		StoreTimestamp: time.Now().UTC().UnixNano(),
	})
}

func (s *ShedStore) Get(addr storage.Address) (c storage.Chunk, err error) {
	batch := new(leveldb.Batch)

	// Get the chunk data and storage timestamp.
	item, err := s.retrievalIndex.Get(shed.Item{
		Address: addr,
	})
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, storage.ErrChunkNotFound
		}
		return nil, err
	}

	// Get the chunk access timestamp.
	accessItem, err := s.accessIndex.Get(shed.Item{
		Address: addr,
	})
	switch err {
	case nil:
		// Remove gc index entry if access timestamp is found.
		err = s.gcIndex.DeleteInBatch(batch, shed.Item{
			Address:         item.Address,
			StoreTimestamp:  accessItem.AccessTimestamp,
			AccessTimestamp: item.StoreTimestamp,
		})
		if err != nil {
			return nil, err
		}
	case leveldb.ErrNotFound:
	// Access timestamp is not found. Do not do anything.
	// This is the firs get request.
	default:
		return nil, err
	}

	// Specify new access timestamp
	accessTimestamp := time.Now().UTC().UnixNano()

	// Put new access timestamp in access index.
	err = s.accessIndex.PutInBatch(batch, shed.Item{
		Address:         addr,
		AccessTimestamp: accessTimestamp,
	})
	if err != nil {
		return nil, err
	}

	// Put new access timestamp in gc index.
	err = s.gcIndex.PutInBatch(batch, shed.Item{
		Address:         item.Address,
		AccessTimestamp: accessTimestamp,
		StoreTimestamp:  item.StoreTimestamp,
	})
	if err != nil {
		return nil, err
	}

	// Write the batch.
	err = s.db.WriteBatch(batch)
	if err != nil {
		return nil, err
	}

	// Return the chunk.
	return storage.NewChunk(item.Address, item.Data), nil
}

func (s *ShedStore) Delete(addr storage.Address) (err error) {
	return s.retrievalIndex.Delete(shed.Item{
		Address: addr,
	})
}

// CollectGarbage is an example of index iteration.
// It provides no reliable garbage collection functionality.
func (s *ShedStore) CollectGarbage() (err error) {
	const maxTrashSize = 100
	maxRounds := 10 // arbitrary number, needs to be calculated

	// Run a few gc rounds.
	for roundCount := 0; roundCount < maxRounds; roundCount++ {
		var garbageCount int
		// New batch for a new cg round.
		trash := new(leveldb.Batch)
		// Iterate through all index items and break when needed.
		err = s.gcIndex.Iterate(func(item shed.Item) (stop bool, err error) {
			// Remove the chunk.
			err = s.retrievalIndex.DeleteInBatch(trash, item)
			if err != nil {
				return false, err
			}
			// Remove the element in gc index.
			err = s.gcIndex.DeleteInBatch(trash, item)
			if err != nil {
				return false, err
			}
			// Remove the relation in access index.
			err = s.accessIndex.DeleteInBatch(trash, item)
			if err != nil {
				return false, err
			}
			garbageCount++
			if garbageCount >= maxTrashSize {
				return true, nil
			}
			return false, nil
		}, nil)
		if err != nil {
			return err
		}
		if garbageCount == 0 {
			return nil
		}
		err = s.db.WriteBatch(trash)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the underlying database.
func (s *ShedStore) Close() error {
	return s.db.Close()
}
