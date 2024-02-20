package main

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/odvarkadaniel/hashring-go"
)

type Server string

func (s *Server) String() string {
	return string(*s)
}

func main() {
	cfg := hashring.Config{
		PartitionCount:    71,
		ReplicationFactor: 20,
		Hasher:            xxhash.Sum64,
	}

	buckets := []hashring.Bucket{}

	for i := 0; i < 8; i++ {
		member := Server(fmt.Sprintf("node%d", i))
		buckets = append(buckets, &member)
	}

	ring := hashring.New(cfg, buckets)

	owners := make(map[string]int)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owner := ring.GetPartitionBucket(partID)
		fmt.Println(owner.String())
		owners[owner.String()]++
	}
	fmt.Println("average load:", ring.AvgLoad())
	fmt.Println("owners:", owners)
	fmt.Println(ring.Buckets())
	fmt.Println(ring.GetLoads())

	// fmt.Println(ring)
	// fmt.Println(ring.Buckets())
	// key := "test-key"

	// bucket := ring.Get(key)
	// fmt.Println(bucket.String())
}
