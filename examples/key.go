package main

import (
	"fmt"
	"hash/fnv"

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
		PartitionCount:    23,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}

	buckets := []hashring.Bucket{}

	for i := 0; i < 8; i++ {
		b := Server(fmt.Sprintf("node%d", i))
		buckets = append(buckets, &b)
	}

	ring := hashring.New(cfg, buckets)

	key := "test-key"
	bucket := ring.Get(key)

	// This should print node7.
	fmt.Println(bucket.String())
}
