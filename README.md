# hashring-go
This is a library that provides consistent hashing with bounded loads.

## Overview
The package makes sure that all the keys are distributed uniformly and we operate consistently. We offer all the standard functionality such as `Add`, `Remove` and `Get` operations.
The user can configure what the `ReplicationFactor` and `PartitionCount` should be -- bear in mind, that these values can not be chaned after initializing the hashring object with them.
Before rushing in, try to find a combination of values that suits your needs -- also make sure, that your hashing function is uniformly distributing the values around the ring for best performance. 

## Contributing
Before creating a pull request, create an issue first.

## Install
```
go get github.com/odvarkadaniel/hashring-go
```
You can find some usage examples in the `examples` folder.

## Documentation
To see a list of all functions, please visit /TODO: Link/.

## Examples
As mentioned before, more examples can be seen in the `examples` folder.
```go
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

    // Create our fake servers
	for i := 0; i < 8; i++ {
		b := Server(fmt.Sprintf("node%d", i))
		buckets = append(buckets, &b)
	}

    // Create a new HashRing object
	ring := hashring.New(cfg, buckets)

	key := "test-key"
    // Get a bucket that holds the key
	bucket := ring.Get(key)

	// This should print node7.
	fmt.Println(bucket.String())
}
```

## License
This project uses `MIT LICENSE`, for more details, please see the `LICENSE` file.