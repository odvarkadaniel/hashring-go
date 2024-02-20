package hashring

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"testing"
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

func TestAdd(t *testing.T) {
	cfg := &Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}
	c := New(*cfg, nil)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := Server(fmt.Sprintf("node%d", i))
		members[member.String()] = struct{}{}
		c.Add(&member)
	}
	for member := range members {
		found := false
		for _, mem := range c.getBuckets() {
			if member == mem.String() {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s could not be found", member)
		}
	}
}

func BenchmarkAddRemove(b *testing.B) {
	cfg := &Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}
	c := New(*cfg, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := Server("node" + strconv.Itoa(i))
		c.Add(&member)
		c.Remove(member.String())
	}
}

func BenchmarkLocateKey(b *testing.B) {
	cfg := &Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}
	c := New(*cfg, nil)
	s1 := Server("node1")
	c.Add(&s1)
	s2 := Server("node2")
	c.Add(&s2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		c.Get(key)
	}
}

// func BenchmarkGetClosestN(b *testing.B) {
// 	cfg := &Config{
// 		PartitionCount:    23,
// 		ReplicationFactor: 20,
// 		Hasher:            xxhash.Sum64,
// 	}
// 	c := New(*cfg, nil)
// 	for i := 0; i < 10; i++ {
// 		s1 := Server(fmt.Sprintf("node%d", i))
// 		c.Add(&s1)
// 	}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		key := []byte("key" + strconv.Itoa(i))
// 		_, _ = c.GetClosestN(key, 3)
// 	}
// }
