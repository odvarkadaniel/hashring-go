package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/odvarkadaniel/hashring-go"
)

type Server string

func (s *Server) String() string {
	return string(*s)
}

func Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func main() {
	cfg := hashring.Config{
		PartitionCount:    113,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}

	buckets := []hashring.Bucket{}

	for i := 0; i < 8; i++ {
		b := Server(fmt.Sprintf("node-%d-%d", rand.Intn(1000), i))
		buckets = append(buckets, &b)
	}

	ring := hashring.New(cfg, buckets)

	owner := map[string]int{}
	for i := 0; i < cfg.PartitionCount; i++ {
		bucket := ring.GetPartitionBucket(i)
		owner[bucket.String()]++
	}

	fmt.Println("Average load:", ring.AvgLoad())
	fmt.Println("Partitioned owners:", owner)
}
