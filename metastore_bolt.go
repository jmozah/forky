package forky

import (
	"github.com/ethersphere/swarm/chunk"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketNameMeta = []byte("Meta")
	bucketNameFree = []byte("Free")
)

type BoltMetaStore struct {
	db *bolt.DB
}

func NewBoltMetaStore(filename string) (s *BoltMetaStore, err error) {
	db, err := bolt.Open(filename, 0666, &bolt.Options{
		NoSync: true,
	})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucketNameMeta)
		return err
	}); err != nil {
		return nil, err
	}
	return &BoltMetaStore{db: db}, err
}

func (s *BoltMetaStore) Get(addr chunk.Address) (m *Meta, err error) {
	if err := s.db.View(func(tx *bolt.Tx) (err error) {
		data := tx.Bucket(bucketNameMeta).Get(addr)
		if data == nil {
			return chunk.ErrChunkNotFound
		}
		m = new(Meta)
		if err := m.UnmarshalBinary(data); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *BoltMetaStore) Put(addr chunk.Address, shard uint8, redeemed bool, m *Meta) (err error) {
	meta, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		return tx.Bucket(bucketNameMeta).Put(addr, meta)
	})
}

func (s *BoltMetaStore) Free(shard uint8) (offset int64, err error) {
	return 0, nil
}

func (s *BoltMetaStore) Delete(addr chunk.Address, shard uint8) (err error) {
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		return tx.Bucket(bucketNameMeta).Delete(addr)
	})
}

func (s *BoltMetaStore) Close() (err error) {
	return s.db.Close()
}
