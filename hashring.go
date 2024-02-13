package hashring

import "sync"

type Config struct {
	Hasher interface{}

	// TODO: rename?
	//
	ReplicationFactor int
}

type Bucket interface {
	String() string
}

type HashRing struct {
	mu sync.RWMutex

	config Config

	// Maybe just func func(x []byte) uint64
	// Or an actual interface
	hasher interface{}
}

func New(config *Config) *HashRing {
	return nil
}
