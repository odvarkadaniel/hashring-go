package hashring

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
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
		partitions:        make(map[int]Bucket),
		partitionCount:    uint64(config.PartitionCount),
		replicationFactor: config.ReplicationFactor,
	}

	for _, b := range buckets {
		hr.add(b)
	}

	if len(hr.buckets) > 0 {
		hr.distibute()
	}

	return hr
}

func (hr *HashRing) distibute() {
	loads := make(map[string]float64)
	partitions := make(map[int]Bucket)
	bs := make([]byte, 8)

	for i := uint64(0); i < hr.partitionCount; i++ {
		fmt.Println(i, "---", hr.partitionCount)
		binary.LittleEndian.PutUint64(bs, i)

		key := hr.hasher(bs)

		fmt.Println("Key:", key)

		idx := sort.Search(len(hr.sortedSet), func(i int) bool {
			return hr.sortedSet[i] >= key
		})
		if idx >= len(hr.sortedSet) {
			idx = 0
		}

		avgLoad := hr.avgLoad()

		count := 0

		for {
			count++
			if count >= len(hr.sortedSet) {
				panic("sorted set bla bla")
			}

			h := hr.sortedSet[idx]

			bucket := hr.ring[h]
			load := loads[bucket.String()] + 1
			fmt.Println(load, "---", avgLoad)
			if load <= avgLoad {
				fmt.Println("here")
				partitions[int(i)] = bucket
				loads[bucket.String()]++

				hr.partitions = partitions
				hr.loads = loads

				break
			}

			idx++
			if idx >= len(hr.sortedSet) {
				idx = 0
			}
		}

	}

	hr.partitions = partitions
	hr.loads = loads
}

// Add adds a new bucket to the consistent hash ring.
func (hr *HashRing) Add(bucket Bucket) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.add(bucket)

	hr.distibute()
}

func (hr *HashRing) add(bucket Bucket) {
	if _, ok := hr.buckets[bucket.String()]; ok {
		// TODO: Log that we already have this member?
		return
	}

	for i := 0; i < hr.replicationFactor; i++ {
		key := fmt.Sprintf("%s%d", bucket.String(), i)
		hash := hr.hasher([]byte(key))

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
		// TODO: Log that the member doesn't exist?
		return
	}

	for i := 0; i < hr.replicationFactor; i++ {
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
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getPartitionID(key)
}

func (hr *HashRing) getPartitionID(key string) int {
	hash := hr.hasher([]byte(key))

	return int(hash % uint64(hr.partitionCount))
}

// GetPartitionBucket gets a bucket for a given partition id.
func (hr *HashRing) GetPartitionBucket(id int) Bucket {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	return hr.getPartitionBucket(id)
}

func (hr *HashRing) getPartitionBucket(id int) Bucket {
	if bucket, ok := hr.partitions[id]; !ok {
		fmt.Println("returning nil")
		return nil
	} else {
		fmt.Println("returning")
		return bucket
	}
}

// Get returns a bucket that is associated to a given key.
func (hr *HashRing) Get(key string) Bucket {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	id := hr.getPartitionID(key)

	return hr.getPartitionBucket(id)
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

// func (hr *HashRing) dec(bucket Bucket) {
// 	hr.mu.Lock()
// 	defer hr.mu.Unlock()

// 	if _, ok := hr.buckets[bucket.String()]; !ok {
// 		return
// 	}

// 	hr.loads[bucket.String()] -= 1
// }

// func (hr *HashRing) inc(bucket Bucket) {
// 	hr.mu.Lock()
// 	defer hr.mu.Unlock()

// 	if _, ok := hr.buckets[bucket.String()]; !ok {
// 		return
// 	}

// 	hr.loads[bucket.String()] += 1
// }
