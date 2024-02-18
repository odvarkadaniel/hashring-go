package hashring

import (
	"fmt"
	"sync"
)

// TODO: Make these values sensible.
const (
	DefaultPartitionCount    = 13
	DefaultReplicationFactor = 20
)

type HashFn func([]byte) uint64

type Config struct {
	// Hasher is the hash function that we use to disribute the keys
	// in the hash ring.
	Hasher HashFn

	// ReplicationFactor determines the number of virtual nodes we
	// put into the hash ring (replication of a real node on the ring).
	ReplicationFactor int

	// PartitionCount determines how many time we want to partition a key.
	PartitionCount int
}

type Bucket interface {
	String() string
}

type HashRing struct {
	mu sync.RWMutex

	hasher HashFn
	// sortedSet         []uint64
	partitionCount    uint64
	ReplicationFactor uint64

	buckets    map[string]Bucket
	partitions map[int]Bucket
	ring       map[uint64]Bucket
}

// New returns a pointer a HashRing structure.
func New(config Config, buckets []Bucket) *HashRing {
	if config.Hasher == nil {
		panic("hasher can not be nil")
	}

	hr := &HashRing{
		hasher:     config.Hasher,
		ring:       make(map[uint64]Bucket),
		buckets:    make(map[string]Bucket),
		partitions: make(map[int]Bucket),
	}

	if config.PartitionCount == 0 {
		hr.partitionCount = DefaultPartitionCount
	}

	if config.ReplicationFactor == 0 {
		hr.ReplicationFactor = DefaultReplicationFactor
	}

	for _, b := range buckets {
		hr.add(b)
	}

	if len(hr.buckets) > 0 {
		hr.distibute()
	}

	return hr
}

// TODO: Implement the functionality.
func (hr *HashRing) distibute() {
	// for i := uint64(0); i < hr.partitionCount; i++ {
	// }
}

// Add adds a new bucket to the consistent hash ring.
func (hr *HashRing) Add(bucket Bucket) {
	hr.add(bucket)
}

func (hr *HashRing) add(bucket Bucket) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, ok := hr.buckets[bucket.String()]; ok {
		// TODO: Log that we already have this member?
		return
	}

	for i := uint64(0); i < hr.ReplicationFactor; i++ {
		key := fmt.Sprintf("%s%d", bucket.String(), i)
		hash := hr.hasher([]byte(key))

		hr.ring[hash] = bucket
	}

	hr.buckets[bucket.String()] = bucket

	hr.distibute()
}

// Remove removes a bucket from the consistent hash ring.
func (hr *HashRing) Remove(key string) {
	hr.remove(key)
}

func (hr *HashRing) remove(key string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, ok := hr.buckets[key]; !ok {
		// TODO: Log that the member doesn't exist?
		return
	}

	for i := uint64(0); i < hr.ReplicationFactor; i++ {
		key := fmt.Sprintf("%s%d", key, i)
		hash := hr.hasher([]byte(key))

		delete(hr.ring, hash)
	}

	delete(hr.buckets, key)

	hr.distibute()
}

// Buckets returns all buckets that exist in the hash ring.
func (hr *HashRing) Buckets() []Bucket {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getBuckets()
}

func (hr *HashRing) getBuckets() []Bucket {
	buckets := []Bucket{}

	for _, b := range hr.buckets {
		buckets = append(buckets, b)
	}

	return buckets
}

// TODO: Implement the functionality.
func (hr *HashRing) GetClosestN(key string, n int) ([]Bucket, error) {
	return nil, nil
}

// GetPartitionID gets a partition id for a given key in the ring.
func (hr *HashRing) GetPartitionID(key string) int {
	return hr.getPartitionID(key)
}

func (hr *HashRing) getPartitionID(key string) int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	hash := hr.hasher([]byte(key))

	return int(hash % hr.partitionCount)
}

// GetPartitionBucket gets a bucket for a given partition id.
func (hr *HashRing) GetPartitionBucket(id int) Bucket {
	return hr.getPartitionBucket(id)
}

func (hr *HashRing) getPartitionBucket(id int) Bucket {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if bucket, ok := hr.partitions[id]; !ok {
		return nil
	} else {
		return bucket
	}
}

// Get returns a bucket that is associated to a given key.
func (hr *HashRing) Get(key string) Bucket {
	id := hr.getPartitionID(key)

	return hr.getPartitionBucket(id)
}
