package forky

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ethersphere/swarm/chunk"
)

var _ store = new(DB)

var ErrDBClosed = errors.New("closed database")

type store interface {
	Get(addr chunk.Address) (ch chunk.Chunk, err error)
	Put(ch chunk.Chunk) (err error)
	Delete(addr chunk.Address) (err error)
	Close() (err error)
}

const shardCount = 8

type DB struct {
	chunks   map[uint8]*os.File
	shardsMu map[uint8]*sync.Mutex
	meta     MetaStore
	free     map[uint8]struct{}
	freeMu   sync.RWMutex
	wg       sync.WaitGroup
	quit     chan struct{}
	quitOnce sync.Once
}

func NewDB(path string, metaStore MetaStore) (db *DB, err error) {
	chunks := make(map[byte]*os.File, shardCount)
	shardsMu := make(map[uint8]*sync.Mutex)
	for i := byte(0); i <= shardCount-1; i++ {
		chunks[i], err = os.OpenFile(filepath.Join(path, fmt.Sprintf("chunk%v.db", i)), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		shardsMu[i] = new(sync.Mutex)
	}
	return &DB{
		chunks:   chunks,
		shardsMu: shardsMu,
		meta:     metaStore,
		free:     make(map[uint8]struct{}),
		quit:     make(chan struct{}),
	}, nil
}

func (db *DB) protect() (done func(), err error) {
	select {
	case <-db.quit:
		return nil, ErrDBClosed
	default:
	}
	db.wg.Add(1)
	return db.wg.Done, nil
}

func (db *DB) Get(addr chunk.Address) (ch chunk.Chunk, err error) {
	done, err := db.protect()
	if err != nil {
		return nil, err
	}
	defer done()

	m, err := db.meta.Get(addr)
	if err != nil {
		return nil, err
	}
	data := make([]byte, m.Size)
	_, err = db.chunks[db.shard(addr)].ReadAt(data, m.Offset)
	if err != nil {
		return nil, err
	}
	return chunk.NewChunk(addr, data), nil
}

func (db *DB) Put(ch chunk.Chunk) (err error) {
	done, err := db.protect()
	if err != nil {
		return err
	}
	defer done()

	addr := ch.Address()
	shard := db.shard(addr)
	f := db.chunks[shard]
	data := ch.Data()
	section := make([]byte, chunk.DefaultSize)
	copy(section, data)

	db.freeMu.RLock()
	_, hasFree := db.free[shard]
	db.freeMu.RUnlock()

	var offset int64
	var redeemed bool
	mu := db.shardsMu[shard]
	mu.Lock()
	if hasFree {
		freeOffset, err := db.meta.Free(shard)
		if err != nil {
			return err
		}
		if freeOffset < 0 {
			offset, err = f.Seek(0, io.SeekEnd)
			if err != nil {
				mu.Unlock()
				return err
			}
			db.freeMu.Lock()
			delete(db.free, shard)
			db.freeMu.Unlock()
		} else {
			offset, err = f.Seek(freeOffset, io.SeekStart)
			if err != nil {
				mu.Unlock()
				return err
			}
			redeemed = true
		}
	} else {
		offset, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			mu.Unlock()
			return err
		}
	}
	_, err = f.Write(section)
	if err != nil {
		mu.Unlock()
		return err
	}
	if redeemed {
		defer mu.Unlock()
	} else {
		mu.Unlock()
	}

	return db.meta.Put(addr, shard, redeemed, &Meta{
		Size:   uint16(len(data)),
		Offset: int64(offset),
	})
}

func (db *DB) Delete(addr chunk.Address) (err error) {
	done, err := db.protect()
	if err != nil {
		return err
	}
	defer done()

	shard := db.shard(addr)
	db.freeMu.Lock()
	db.free[shard] = struct{}{}
	db.freeMu.Unlock()
	return db.meta.Delete(addr, shard)
}

func (db *DB) Close() (err error) {
	db.quitOnce.Do(func() {
		close(db.quit)
	})
	done := make(chan struct{})
	go func() {
		db.wg.Done()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
	for _, f := range db.chunks {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return db.meta.Close()
}

func (db *DB) shard(addr chunk.Address) (shard uint8) {
	shard = addr[len(addr)-1] % shardCount
	return shard
}

type Meta struct {
	Size   uint16
	Offset int64
}

func (m *Meta) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 10)
	binary.BigEndian.PutUint64(data[:8], uint64(m.Offset))
	binary.BigEndian.PutUint16(data[8:10], uint16(m.Size))
	return data, nil
}

func (m *Meta) UnmarshalBinary(data []byte) error {
	m.Offset = int64(binary.BigEndian.Uint64(data[:8]))
	m.Size = binary.BigEndian.Uint16(data[8:10])
	return nil
}

type MetaStore interface {
	Put(addr chunk.Address, shard uint8, redeemed bool, m *Meta) error
	Get(addr chunk.Address) (*Meta, error)
	Delete(addr chunk.Address, shard uint8) error
	Free(shard uint8) (int64, error)
	Close() error
}
