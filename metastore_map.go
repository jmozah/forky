package forky

import (
	"encoding/hex"
	"sync"

	"github.com/ethersphere/swarm/chunk"
)

type MapMetaStore struct {
	m  map[string]*Meta
	mu sync.RWMutex
}

func NewMapMetaStore() (s *MapMetaStore) {
	return &MapMetaStore{
		m: make(map[string]*Meta),
	}
}

func (s *MapMetaStore) Get(addr chunk.Address) (m *Meta, err error) {
	s.mu.RLock()
	m = s.m[hex.EncodeToString(addr)]
	s.mu.RUnlock()
	return m, nil
}

func (s *MapMetaStore) Put(addr chunk.Address, m *Meta) (err error) {
	s.mu.Lock()
	s.m[hex.EncodeToString(addr)] = m
	s.mu.Unlock()
	return nil
}

func (s *MapMetaStore) Close() (err error) {
	return nil
}
