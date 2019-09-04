package forky

import (
	"encoding/binary"
	"sync"

	"github.com/ethersphere/swarm/chunk"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDBMetaStore struct {
	db        *leveldb.DB
	metaCache map[string]*Meta
	freeCache map[uint8]map[int64]struct{}
	mu        sync.RWMutex
}

func NewLevelDBMetaStore(filename string, shardCount uint8) (s *LevelDBMetaStore, err error) {
	db, err := leveldb.OpenFile(filename, &opt.Options{})
	if err != nil {
		return nil, err
	}
	freeCache := make(map[uint8]map[int64]struct{})
	for i := uint8(0); i < shardCount; i++ {
		freeCache[i] = make(map[int64]struct{})
	}
	return &LevelDBMetaStore{
		db:        db,
		metaCache: make(map[string]*Meta),
		freeCache: freeCache,
	}, err
}

func (s *LevelDBMetaStore) Get(addr chunk.Address) (m *Meta, err error) {
	key := string(addr)
	s.mu.RLock()
	m, ok := s.metaCache[key]
	if ok {
		s.mu.RUnlock()
		return m, nil
	}
	s.mu.RUnlock()
	data, err := s.db.Get(chunkKey(addr), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, chunk.ErrChunkNotFound
		}
		return nil, err
	}
	m = new(Meta)
	if err := m.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.metaCache[key] = m
	s.mu.Unlock()
	return m, nil
}

func (s *LevelDBMetaStore) Put(addr chunk.Address, shard uint8, redeemed bool, m *Meta) (err error) {
	batch := new(leveldb.Batch)
	if redeemed {
		batch.Delete(freeKey(shard, m.Offset))
		s.mu.Lock()
		delete(s.freeCache[shard], m.Offset)
		s.mu.Unlock()
	}
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	batch.Put(chunkKey(addr), meta)
	s.mu.Lock()
	s.metaCache[string(addr)] = m
	s.mu.Unlock()
	return s.db.Write(batch, nil)
}

func (s *LevelDBMetaStore) Free(shard uint8) (offset int64, err error) {
	s.mu.RLock()
	for o := range s.freeCache[shard] {
		s.mu.RUnlock()
		return o, nil
	}
	s.mu.RUnlock()

	i := s.db.NewIterator(nil, nil)
	defer i.Release()

	i.Seek([]byte{freePrefix, shard})
	key := i.Key()
	if key == nil || key[0] != freePrefix || key[1] != shard {
		return -1, nil
	}
	offset = int64(binary.BigEndian.Uint64(key[2:10]))
	s.mu.Lock()
	s.freeCache[shard][offset] = struct{}{}
	s.mu.Unlock()
	return offset, nil
}

func (s *LevelDBMetaStore) Delete(addr chunk.Address, shard uint8) (err error) {
	data, err := s.db.Get(chunkKey(addr), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return chunk.ErrChunkNotFound
		}
		return err
	}
	m := new(Meta)
	if err := m.UnmarshalBinary(data); err != nil {
		return err
	}
	batch := new(leveldb.Batch)
	batch.Put(freeKey(shard, m.Offset), nil)
	s.mu.Lock()
	s.freeCache[shard][m.Offset] = struct{}{}
	delete(s.metaCache, string(addr))
	batch.Delete(chunkKey(addr))
	s.mu.Unlock()
	return s.db.Write(batch, nil)
}

func (s *LevelDBMetaStore) Close() (err error) {
	return s.db.Close()
}

const (
	chunkPrefix = 0
	freePrefix  = 1
)

func chunkKey(addr chunk.Address) (key []byte) {
	return append([]byte{chunkPrefix}, addr...)
}

func freeKey(shard uint8, offset int64) (key []byte) {
	key = make([]byte, 10)
	key[0] = freePrefix
	key[1] = shard
	binary.BigEndian.PutUint64(key[2:10], uint64(offset))
	return key
}
