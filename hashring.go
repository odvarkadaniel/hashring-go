package hashring

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"slices"
	"sort"
	"sync"
)

const (
	DefaultPartitionCount    = 71
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

	// PartitionCount determines how many time we want to partition the space
	// to make for more uniform distribution of keys. Select a prime number for
	// this.
	PartitionCount int
}

type Bucket interface {
	String() string
}

type HashRing struct {
	mu sync.RWMutex

	hasher            HashFn
	sortedSet         []uint64
	partitionCount    uint64
	replicationFactor int

	buckets    map[string]Bucket
	loads      map[string]float64
	partitions map[int]Bucket
	ring       map[uint64]Bucket
}

// New returns a pointer a HashRing structure.
func New(config Config, buckets []Bucket) *HashRing {
	if config.Hasher == nil {
		panic("hasher can not be nil")
	}

	if config.PartitionCount == 0 {
		config.PartitionCount = DefaultPartitionCount
	}

	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}

	hr := &HashRing{
		hasher:            config.Hasher,
		sortedSet:         []uint64{},
		ring:              make(map[uint64]Bucket),
		buckets:           make(map[string]Bucket),
		partitionCount:    uint64(config.PartitionCount),
		replicationFactor: config.ReplicationFactor,
	}

	for _, b := range buckets {
		hr.add(b)
	}

	if len(hr.buckets) > 0 {
		if err := hr.distribute(); err != nil {
			log.Fatalf("failed to initialize hashring: %v", err)
			return nil
		}
	}

	return hr
}

func (hr *HashRing) distribute() error {
	loads := make(map[string]float64)
	partitions := make(map[int]Bucket)
	partitionCountBytes := make([]byte, 8)

	avgLoad := hr.avgLoad()

	for i := uint64(0); i < hr.partitionCount; i++ {
		binary.LittleEndian.PutUint64(partitionCountBytes, i)

		hash := hr.hasher(partitionCountBytes)

		idx := sort.Search(len(hr.sortedSet), func(i int) bool {
			return hr.sortedSet[i] >= hash
		})
		if idx >= len(hr.sortedSet) || idx < 0 {
			idx = 0
		}

		count := 0

		// Find bucket with free space to hold partition.
		for count < len(hr.sortedSet) {
			count++
			if count >= len(hr.sortedSet) {
				return fmt.Errorf("could not distribute partitions - try to increase bucket count")
			}

			h := hr.sortedSet[idx]

			bucket := hr.ring[h]
			load := loads[bucket.String()] + 1
			if load <= avgLoad {
				partitions[int(i)] = bucket
				loads[bucket.String()]++

				break
			}

			// "Wrap" around the ring.
			idx++
			if idx >= len(hr.sortedSet) {
				idx = 0
			}
		}
	}

	hr.partitions = partitions
	hr.loads = loads

	return nil
}

// Add adds a new bucket to the consistent hash ring.
func (hr *HashRing) Add(bucket Bucket) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.add(bucket)

	if err := hr.distribute(); err != nil {
		log.Fatal(err)
	}
}

func (hr *HashRing) add(bucket Bucket) {
	if _, ok := hr.buckets[bucket.String()]; ok {
		return
	}

	for i := 0; i < hr.replicationFactor; i++ {
		hash := hr.hasher([]byte(fmt.Sprintf("%s%d", bucket.String(), i)))

		hr.ring[hash] = bucket
		hr.sortedSet = append(hr.sortedSet, hash)
	}

	slices.Sort(hr.sortedSet)

	hr.buckets[bucket.String()] = bucket
}

// Remove removes a bucket from the consistent hash ring.
func (hr *HashRing) Remove(key string) {
	hr.remove(key)
}

func (hr *HashRing) remove(key string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, ok := hr.buckets[key]; !ok {
		return
	}

	for i := 0; i < hr.replicationFactor; i++ {
		hash := hr.hasher([]byte(fmt.Sprintf("%s%d", key, i)))

		delete(hr.ring, hash)

		hr.removeElement(hash)
	}

	delete(hr.buckets, key)
	if len(hr.buckets) < 1 {
		hr.partitions = make(map[int]Bucket)
		hr.loads = make(map[string]float64)
		return
	}

	if err := hr.distribute(); err != nil {
		log.Fatal(err)
	}
}

func (hr *HashRing) removeElement(val uint64) {
	i, _ := slices.BinarySearch(hr.sortedSet, val)
	hr.sortedSet = append(hr.sortedSet[:i], hr.sortedSet[i+1:]...)
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

// GetPartitionID gets a partition id for a given key in the ring.
func (hr *HashRing) GetPartitionID(key string) int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getPartitionID(key)
}

func (hr *HashRing) getPartitionID(key string) int {
	return int(hr.hasher([]byte(key)) % uint64(hr.partitionCount))
}

// GetPartitionBucket gets a bucket for a given partition id.
func (hr *HashRing) GetPartitionBucket(id int) Bucket {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getPartitionBucket(id)
}

func (hr *HashRing) getPartitionBucket(id int) Bucket {
	if bucket, ok := hr.partitions[id]; !ok {
		return nil
	} else {
		return bucket
	}
}

// Get returns a bucket that is associated to a given key.
func (hr *HashRing) Get(key string) Bucket {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getPartitionBucket(hr.getPartitionID(key))
}

// GetLoads returns a mapping of each bucket to it load.
func (hr *HashRing) GetLoads() map[string]float64 {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getLoads()
}

func (hr *HashRing) getLoads() map[string]float64 {
	loads := map[string]float64{}

	for k, v := range hr.loads {
		loads[k] = v
	}

	return loads
}

func (hr *HashRing) AvgLoad() float64 {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.avgLoad()
}

func (hr *HashRing) avgLoad() float64 {
	if len(hr.buckets) == 0 {
		return 0
	}

	return math.Ceil(float64(hr.partitionCount/uint64(len(hr.buckets))) * 1.25)
}
