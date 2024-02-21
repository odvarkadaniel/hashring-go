// MIT License
//
// Copyright (c) 2024 Odvarka Daniel
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
	ring := New(*cfg, nil)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := Server(fmt.Sprintf("node%d", i))
		members[member.String()] = struct{}{}
		ring.Add(&member)
	}
	for member := range members {
		found := false
		for _, mem := range ring.getBuckets() {
			if member == mem.String() {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s could not be found", member)
		}
	}
}

func TestRemove(t *testing.T) {
	cfg := &Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}
	ring := New(*cfg, nil)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := Server(fmt.Sprintf("node%d", i))
		members[member.String()] = struct{}{}
		ring.Add(&member)
	}

	ring.Remove("node3")

	if len(ring.Buckets()) != 7 {
		t.Fatalf("Failed to remove a server from the ring")
	}
}

func TestGetKey(t *testing.T) {
	cfg := &Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Hasher:            Sum64,
	}
	ring := New(*cfg, nil)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := Server(fmt.Sprintf("node%d", i))
		members[member.String()] = struct{}{}
		ring.Add(&member)
	}

	if res := ring.Get("my-key"); res == nil {
		t.Fatal("Failed to get a key from the ring")
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
