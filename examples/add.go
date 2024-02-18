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
		PartitionCount:    7,
		ReplicationFactor: 2,
		Hasher:            xxhash.Sum64,
	}

	buckets := []hashring.Bucket{}

	for i := 0; i < 5; i++ {
		member := Server(fmt.Sprintf("node%d", i))
		buckets = append(buckets, &member)
	}

	ring := hashring.New(cfg, buckets)
	fmt.Println(ring)
	// key := "test-key"

	// bucket := ring.Get(key)
	// fmt.Println(bucket.String())
}
