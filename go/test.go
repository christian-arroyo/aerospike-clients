package main

import (
	"bytes"
	"fmt"
	"log"

	aero "github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
)

// This is only for this example.
// Please handle errors properly.
func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	var buf bytes.Buffer
	logger := log.New(&buf, "logger: ", log.Lshortfile)
	asl.Logger.SetLogger(logger)
	asl.Logger.SetLevel(asl.DEBUG)
	// define a client to connect to
	clientPolicy := aero.NewClientPolicy()
	clientPolicy.UseServicesAlternate = true
	client, err := aero.NewClientWithPolicy(clientPolicy, "127.0.0.1", 3104)
	panicOnError(err)

	key, err := aero.NewKey("test", "aerospike", "key")
	panicOnError(err)

	// define some bins with data
	bins := aero.BinMap{
		"bin1": 42,
		"bin2": "An elephant is a mouse with an operating system",
		"bin3": []any{"Go", 2009},
	}

	// write the bins
	err = client.Put(nil, key, bins)
	panicOnError(err)

	// read it back!
	rec, err := client.Get(nil, key)
	fmt.Printf(rec.String())
	panicOnError(err)

	// delete the key, and check if key exists
	existed, err := client.Delete(nil, key)
	panicOnError(err)
	fmt.Printf("Record existed before delete? %v\n", existed)
}
