package main

import (
	"fmt"
	"log"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Global variables
var namespace string = "test"
var set string = "testSet"
var ip_address string = "127.0.0.1"
var port int = 3101

// This is only for this example.
// Please handle errors properly.
func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	// Initialize client
	clientPolicy := as.NewClientPolicy()
	clientPolicy.UseServicesAlternate = true
	clientPolicy.MinConnectionsPerNode = 10
	client, err := as.NewClientWithPolicy(clientPolicy, ip_address, port)
	panicOnError(err)

	// Insert sample records with TTL of 86400 seconds (1 week)
	oneDayPolicy := as.NewWritePolicy(0, 0)
	oneDayPolicy.Expiration = uint32(86400)
	putBins(client, nil, "key1", as.BinMap{"ID": 1})
	putBins(client, nil, "key2", as.BinMap{"ID": 3})
	putBins(client, nil, "key3", as.BinMap{"ID": 80})

	// Insert sample records setting TTL to 604800 seconds (1 day)
	oneWeekPolicy := as.NewWritePolicy(0, 0)
	oneWeekPolicy.Expiration = uint32(604800)
	putBins(client, nil, "key4", as.BinMap{"ID": 2})
	putBins(client, nil, "key5", as.BinMap{"ID": 5})
	putBins(client, nil, "key6", as.BinMap{"ID": 100})

	statement := as.NewStatement(namespace, set)

	// Run a PI foreground query and print set
	fmt.Println("Before background query")
	recordSet, err := client.Query(nil, statement)
	if err != nil {
		log.Fatal(err)
	}
	for records := range recordSet.Results() {
		if records != nil {
			fmt.Printf("Record: %v \n", records.Record.Bins)
		}
	}

	// Run a PI background query and delete records that have an "ID" bin with value < 10, and TTL < 30 hours
	queryPolicy := as.NewQueryPolicy()
	queryPolicy.MaxRecords = 20
	wp := as.NewWritePolicy(0, 0)
	wp.FilterExpression = as.ExpAnd(
		as.ExpBinExists("ID"),
		as.ExpLess(as.ExpIntBin("ID"), as.ExpIntVal(10)),
		as.ExpLess(as.ExpTTL(), as.ExpIntVal(30*60*60))) // Filter records with TTL < 30 hours
	task, err := client.QueryExecute(nil, wp, statement, as.DeleteOp())
	panicOnError(err)
	// Not sure if this works
	task.OnComplete()

	// Wait 10 seconds for background query to finish
	time.Sleep(10 * time.Second)

	// Perform same foreground PI query again to compare differece in records
	fmt.Println("After background query")
	recordSet2, err := client.Query(nil, statement)
	if err != nil {
		log.Fatal(err)
	}
	for records := range recordSet2.Results() {
		if records != nil {
			// Do something
			fmt.Printf("Record: %v \n", records.Record.Bins)
		}
	}
}

func putBins(client *as.Client, wp *as.WritePolicy, akey string, bins as.BinMap) {
	key, err := as.NewKey(namespace, set, akey)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if err = client.Put(wp, key, bins); err != nil {
		log.Fatalln(err.Error())
	}
}
